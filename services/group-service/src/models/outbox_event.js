const Model = require("../config/objection");

class OutboxEvent extends Model {
    static get tableName() {
        return "outbox_events";
    }

    static get jsonSchema() {
        return {
            type: "object",
            required: ["idempotency_key", "topic", "event_type", "payload"],
            properties: {
                id:               { type: "integer" },
                idempotency_key:  { type: "string", maxLength: 100 },
                topic:            { type: "string", maxLength: 100 },
                event_type:       { type: "string", maxLength: 100 },
                payload:          { type: "object" },
                status:           { type: "string", enum: ["pending", "published", "failed"] },
                retry_count:      { type: "integer", minimum: 0 },
                max_retries:      { type: "integer", minimum: 0 },
                error_message:    { type: ["string", "null"] },
                locked_at:        { type: ["string", "null"] },
                published_at:     { type: ["string", "null"] },
            },
        };
    }
}

module.exports = OutboxEvent;
