/**
 * Idempotency table cho transaction-service.
 *
 * Consumer Kafka check bảng này trước khi xử lý event.
 * Nếu idempotency_key đã tồn tại → event đã được xử lý → bỏ qua.
 *
 * INSERT vào bảng này được thực hiện trong cùng DB transaction
 * với việc tạo personal transactions — đảm bảo exactly-once semantics
 * dù Kafka deliver event nhiều lần (at-least-once).
 *
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = async function (knex) {
    await knex.schema.createTable("processed_events", (table) => {
        // PRIMARY KEY = idempotency_key từ outbox_events
        // Format: 'group_txn_{group_transaction_id}'
        table.string("idempotency_key", 100).primary();

        table.timestamp("processed_at").notNullable().defaultTo(knex.fn.now());
    });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = async function (knex) {
    await knex.schema.dropTableIfExists("processed_events");
};
