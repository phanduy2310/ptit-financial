const { Kafka } = require("kafkajs");
const Transaction = require("../models/transaction");
const ProcessedEvent = require("../models/processed_event");
const { transaction } = require("../config/objection");

// ─── Cấu hình ────────────────────────────────────────────────────────────────

const TOPIC = "group.transaction.created";
const GROUP_ID = "transaction-service-group-consumer";
const KAFKA_BROKERS = process.env.KAFKA_BROKERS;

const kafka = new Kafka({
    clientId: "transaction-service-consumer",
    brokers: (KAFKA_BROKERS || "kafka:9092").split(","),
    retry: {
        initialRetryTime: 3000,
        retries: 10,
    },
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

// ─── Handler ─────────────────────────────────────────────────────────────────

/**
 * Xử lý một event GROUP_TRANSACTION_CREATED.
 * Tạo personal transaction cho mỗi thành viên có share amount > 0.
 *
 * Idempotency: check processed_events trước khi insert.
 * INSERT transactions + INSERT processed_events trong cùng DB transaction
 * → nếu crash giữa chừng, cả hai đều rollback → lần sau consumer retry an toàn.
 */
async function handleGroupTransactionCreated(payload) {
    const { event_id, group_transaction_id, type, category, date, note, shares } = payload;

    // Validate payload tối thiểu.
    // Throw ValidationError để processMessage phân biệt với runtime error.
    const missingFields = ["event_id", "type", "category", "date"].filter((f) => !payload[f]);
    if (missingFields.length > 0) {
        const err = new Error(
            `Payload thiếu field bắt buộc: [${missingFields.join(", ")}]. ` +
            `event_id=${event_id ?? "N/A"} group_transaction_id=${group_transaction_id ?? "N/A"}`
        );
        err.isValidationError = true;  // flag để processMessage không retry
        throw err;
    }

    // Lọc phòng thủ lần 2 — phòng trường hợp payload cũ chưa lọc sẵn
    const validShares = (shares || []).filter((s) => Number(s.amount) > 0);

    if (validShares.length === 0) {
        console.log(`[CONSUMER] event_id=${event_id} không có share hợp lệ, bỏ qua.`);
        return;
    }

    // Idempotency check — nếu đã xử lý rồi thì bỏ qua
    const alreadyProcessed = await ProcessedEvent.query().findById(event_id);
    if (alreadyProcessed) {
        console.log(`[CONSUMER] event_id=${event_id} đã xử lý trước đó, bỏ qua (duplicate).`);
        return;
    }

    // Insert personal transactions + mark processed trong cùng DB transaction
    // → đảm bảo exactly-once: không thể có trường hợp insert transactions
    //   thành công nhưng processed_events chưa được ghi (hoặc ngược lại)
    await transaction(Transaction.knex(), async (trx) => {
        // Objection bulk insert chỉ hỗ trợ PostgreSQL/SQL Server.
        // Với MySQL dùng knex.batchInsert — sinh ra 1 câu INSERT ... VALUES (...),(...)
        const rows = validShares.map((share) => ({
            user_id:  share.user_id,
            // Personal transaction luôn là expense — shares thể hiện tiền cá nhân bỏ ra,
            // bất kể group transaction là income hay expense (góc nhìn nhóm khác góc nhìn cá nhân)
            type:     "expense",
            category,
            amount:   Number(share.amount),
            date,
            note: note ? `[Nhóm] ${note}` : "[Nhóm]",
        }));
        await Transaction.knex().batchInsert("transactions", rows).transacting(trx);

        // Mark processed — ngăn xử lý lại nếu Kafka deliver event lần 2
        await ProcessedEvent.query(trx).insert({ idempotency_key: event_id });
    });

    console.log(
        `[CONSUMER] event_id=${event_id} group_transaction_id=${group_transaction_id} ` +
        `→ đã tạo ${validShares.length} personal transaction(s).`
    );
}

// ─── Router ──────────────────────────────────────────────────────────────────

const handlers = {
    GROUP_TRANSACTION_CREATED: handleGroupTransactionCreated,
};

async function processMessage({ message }) {
    let payload;

    try {
        payload = JSON.parse(message.value.toString());
    } catch (err) {
        console.error("[CONSUMER] Không parse được message:", message.value?.toString());
        return; // format lỗi vĩnh viễn, không retry
    }

    const { event_type } = payload;
    const handler = handlers[event_type];

    if (!handler) {
        console.warn(`[CONSUMER] Không có handler cho event_type='${event_type}', bỏ qua.`);
        return;
    }

    try {
        await handler(payload);
    } catch (err) {
        if (err.isValidationError) {
            // Lỗi validation — bug ở producer, retry cũng không giải quyết được
            // Commit offset bình thường, log để developer biết và fix
            console.error(`[CONSUMER] Validation error event_type='${event_type}':`, err.message);
            return;
        }
        // Lỗi runtime (DB down, v.v.) — throw để Kafka không commit offset → tự retry
        console.error(`[CONSUMER] Runtime error event_type='${event_type}' event_id='${payload.event_id}':`, err.message);
        throw err;
    }
}

// ─── Lifecycle ───────────────────────────────────────────────────────────────

async function start() {
    try {
        await consumer.connect();
        await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

        await consumer.run({
            eachMessage: processMessage,
        });

        console.log(`[CONSUMER] Đang lắng nghe topic '${TOPIC}' với groupId '${GROUP_ID}'`);
    } catch (err) {
        console.error("[CONSUMER] Không thể khởi động consumer:", err.message);
        // Không throw — service vẫn chạy được, consumer sẽ retry khi restart
    }
}

async function stop() {
    await consumer.disconnect();
    console.log("[CONSUMER] Consumer dừng.");
}

process.on("SIGTERM", stop);
process.on("SIGINT", stop);

module.exports = { start, stop };
