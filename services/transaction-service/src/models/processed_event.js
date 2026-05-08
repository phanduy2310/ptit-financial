const Model = require("../config/objection");

class ProcessedEvent extends Model {
    static get tableName() {
        return "processed_events";
    }

    static get idColumn() {
        // Primary key là idempotency_key, không phải id auto-increment
        return "idempotency_key";
    }

    static get jsonSchema() {
        return {
            type: "object",
            required: ["idempotency_key"],
            properties: {
                idempotency_key: { type: "string", maxLength: 100 },
                processed_at:    { type: ["string", "null"] },
            },
        };
    }
}

module.exports = ProcessedEvent;
