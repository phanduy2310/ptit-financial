/**
 * Outbox pattern table cho group-service.
 *
 * Mỗi khi tạo group transaction, một event được ghi vào bảng này
 * trong cùng DB transaction — đảm bảo atomicity.
 * Outbox worker sẽ poll bảng này và publish lên Kafka.
 *
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = async function (knex) {
    await knex.schema.createTable("outbox_events", (table) => {
        table.increments("id").primary();

        // Dùng để consumer phát hiện duplicate (idempotency)
        // Format: 'group_txn_{group_transaction_id}'
        table.string("idempotency_key", 100).notNullable().unique();

        // Kafka topic sẽ publish tới
        table.string("topic", 100).notNullable();

        // Loại event, dùng để consumer route xử lý đúng handler
        table.string("event_type", 100).notNullable();

        // Toàn bộ data cần gửi, dạng JSON
        table.json("payload").notNullable();

        // pending   → chờ worker publish
        // published → đã publish thành công
        // failed    → đã hết max_retries, cần can thiệp thủ công
        table
            .enu("status", ["pending", "published", "failed"])
            .notNullable()
            .defaultTo("pending");

        // Số lần đã retry, tăng mỗi lần publish thất bại
        table.integer("retry_count").unsigned().notNullable().defaultTo(0);

        // Giới hạn retry trước khi chuyển sang status='failed'
        table.integer("max_retries").unsigned().notNullable().defaultTo(5);

        // Lỗi cuối cùng ghi lại để debug
        table.text("error_message").nullable();

        // Worker set locked_at = NOW() trước khi publish để tránh
        // nhiều worker instance xử lý cùng 1 event (optimistic locking)
        // Worker khác thấy locked_at còn trong threshold thì bỏ qua
        table.timestamp("locked_at").nullable();

        // Thời điểm publish thành công lên Kafka
        table.timestamp("published_at").nullable();

        table.timestamp("created_at").notNullable().defaultTo(knex.fn.now());
    });

    // Index để worker query nhanh: status + retry_count + locked_at
    await knex.schema.raw(`
        CREATE INDEX idx_outbox_events_worker
        ON outbox_events (status, retry_count, locked_at)
    `);
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = async function (knex) {
    await knex.schema.dropTableIfExists("outbox_events");
};
