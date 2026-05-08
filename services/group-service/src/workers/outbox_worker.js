const { Kafka } = require("kafkajs");
const OutboxEvent = require("../models/outbox_event");

// ─── Cấu hình ────────────────────────────────────────────────────────────────

const POLL_INTERVAL_MS = 3000;       // poll mỗi 3 giây
const BATCH_SIZE = 10;               // xử lý tối đa 10 event mỗi lần poll
const LOCK_TIMEOUT_SEC = 60;         // event bị lock quá 60s được coi là stale → retry

const kafka = new Kafka({
    clientId: "group-service-outbox-worker",
    brokers: (process.env.KAFKA_BROKERS || "kafka:9092").split(","),
    // Retry kết nối Kafka khi service khởi động (Kafka có thể chưa sẵn sàng)
    retry: {
        initialRetryTime: 3000,
        retries: 10,
    },
});

const producer = kafka.producer();

// ─── Core logic ──────────────────────────────────────────────────────────────

/**
 * Lấy batch event cần xử lý:
 * - status pending hoặc failed (còn retry)
 * - retry_count < max_retries
 * - không bị lock bởi worker khác (hoặc lock đã hết hạn)
 */
async function fetchPendingEvents() {
    return OutboxEvent.query()
        .where((builder) => {
            builder
                .where("status", "pending")
                .orWhere((b) =>
                    b.where("status", "failed").whereRaw("retry_count < max_retries")
                );
        })
        .where((builder) => {
            builder
                .whereNull("locked_at")
                .orWhereRaw(`locked_at < NOW() - INTERVAL ${LOCK_TIMEOUT_SEC} SECOND`);
        })
        .orderBy("created_at", "asc")
        .limit(BATCH_SIZE);
}

/**
 * Lock event trước khi publish — tránh worker khác xử lý cùng lúc.
 * Dùng whereNull(locked_at) để đảm bảo chỉ lock được nếu chưa ai lock
 * (optimistic locking — tránh race condition khi scale nhiều worker).
 */
async function lockEvent(id) {
    const updated = await OutboxEvent.query()
        .patch({ locked_at: OutboxEvent.knex().fn.now() })
        .where("id", id)
        .where((builder) => {
            builder
                .whereNull("locked_at")
                .orWhereRaw(`locked_at < NOW() - INTERVAL ${LOCK_TIMEOUT_SEC} SECOND`);
        });

    // updated = số row bị ảnh hưởng; 0 nghĩa là worker khác đã lock trước
    return updated > 0;
}

async function markPublished(id) {
    await OutboxEvent.query().patchAndFetchById(id, {
        status: "published",
        locked_at: null,
        published_at: OutboxEvent.knex().fn.now(),
        error_message: null,
    });
}

async function markFailed(id, retryCount, maxRetries, errMessage) {
    const newRetryCount = retryCount + 1;
    // Chỉ chuyển sang 'failed' khi đã hết lượt retry
    const newStatus = newRetryCount >= maxRetries ? "failed" : "pending";

    await OutboxEvent.query()
        .patch({
            status: newStatus,
            retry_count: newRetryCount,
            locked_at: null,   // giải phóng lock để lần poll sau có thể retry
            error_message: errMessage,
        })
        .findById(id);

    if (newStatus === "failed") {
        console.error(
            `[OUTBOX] Event id=${id} đã thất bại sau ${newRetryCount} lần retry. Cần can thiệp thủ công.`
        );
    }
}

async function processEvent(event) {
    // Thử lock — nếu không lock được thì worker khác đang xử lý, bỏ qua
    const locked = await lockEvent(event.id);
    if (!locked) return;

    try {
        await producer.send({
            topic: event.topic,
            messages: [
                {
                    // key = idempotency_key giúp Kafka route cùng group_transaction
                    // vào cùng partition → đảm bảo ordering nếu cần sau này
                    key: event.idempotency_key,
                    // Wrap payload với event_type để consumer route đúng handler
                    value: JSON.stringify({
                        event_type: event.event_type,
                        ...event.payload,
                    }),
                },
            ],
        });

        await markPublished(event.id);
        console.log(`[OUTBOX] Published event id=${event.id} key=${event.idempotency_key}`);
    } catch (err) {
        console.error(`[OUTBOX] Lỗi publish event id=${event.id} (retry ${event.retry_count + 1}/${event.max_retries}):`, err.message);
        await markFailed(event.id, event.retry_count, event.max_retries, err.message);
    }
}

async function poll() {
    try {
        const events = await fetchPendingEvents();
        if (events.length === 0) return;

        console.log(`[OUTBOX] Tìm thấy ${events.length} event cần xử lý`);

        // Xử lý tuần tự để tránh race condition khi lock
        for (const event of events) {
            await processEvent(event);
        }
    } catch (err) {
        // Lỗi ở tầng poll (DB down, v.v.) — log và chờ lần poll tiếp theo
        console.error("[OUTBOX] Lỗi poll:", err.message);
    }
}

// ─── Lifecycle ───────────────────────────────────────────────────────────────

let pollTimer = null;

async function start() {
    try {
        await producer.connect();
        console.log("[OUTBOX] Kafka producer connected. Worker bắt đầu polling...");

        // Poll ngay lần đầu, sau đó theo interval
        await poll();
        pollTimer = setInterval(poll, POLL_INTERVAL_MS);
    } catch (err) {
        console.error("[OUTBOX] Không thể khởi động worker:", err.message);
        // Không throw — để service vẫn chạy được, worker sẽ retry khi restart
    }
}

async function stop() {
    if (pollTimer) {
        clearInterval(pollTimer);
        pollTimer = null;
    }
    await producer.disconnect();
    console.log("[OUTBOX] Worker dừng.");
}

// Graceful shutdown
process.on("SIGTERM", stop);
process.on("SIGINT", stop);

module.exports = { start, stop };
