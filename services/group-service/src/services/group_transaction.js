const GroupTransaction = require("../models/group_transaction");
const { transaction } = require("../config/objection");
const GroupTransactionShare = require("../models/group_transaction_share");
const OutboxEvent = require("../models/outbox_event");

// Topic Kafka — định nghĩa 1 chỗ, worker và consumer dùng chung
const TOPIC_GROUP_TRANSACTION_CREATED = "group.transaction.created";

class GroupTransactionService {

    async createGroupTransactionWithShares({
        group_id,
        user_id,
        type,
        category,
        amount,
        note,
        date,
        shares
    }) {
        return transaction(GroupTransaction.knex(), async (trx) => {
            const groupTransaction = await GroupTransaction.query(trx).insert({
                group_id,
                user_id,
                type,
                category,
                amount,
                note,
                date
            });

            if (shares.length > 0) {
                // Objection bulk insert chỉ hỗ trợ PostgreSQL/SQL Server.
                // Với MySQL dùng knex.batchInsert — sinh ra 1 câu INSERT ... VALUES (...),(...)
                const shareRows = shares.map((share) => ({
                    transaction_id: groupTransaction.id,
                    user_id: share.user_id,
                    amount: Number(share.amount),
                }));
                await GroupTransactionShare.knex().batchInsert("group_transaction_shares", shareRows).transacting(trx);
            }

            // Lọc shares có amount = 0 tại nguồn — consumer không cần xử lý
            const validShares = shares
                .filter((s) => Number(s.amount) > 0)
                .map((s) => ({ user_id: s.user_id, amount: Number(s.amount) }));

            // Chỉ group income mới phát sinh chi tiêu cá nhân — không tạo outbox event thừa
            if (type === "income" && validShares.length > 0) {
                await OutboxEvent.query(trx).insert({
                    idempotency_key: `group_txn_${groupTransaction.id}`,
                    topic: TOPIC_GROUP_TRANSACTION_CREATED,
                    event_type: "GROUP_TRANSACTION_CREATED",
                    payload: {
                        // Consumer dùng event_id làm idempotency key khi check processed_events
                        event_id: `group_txn_${groupTransaction.id}`,
                        group_transaction_id: groupTransaction.id,
                        group_id,
                        type,
                        category,
                        date,
                        note: note || null,
                        shares: validShares   // đã lọc amount > 0
                    },
                });
            }

            return {
                transaction_id: groupTransaction.id
            };
        });
    };

    async getAllTransactions(group_id) {
        return GroupTransaction.query()
            .where("group_id", group_id)
            .orderBy("date", "desc");
    }

    async getById(id) {
        return GroupTransaction.query().findById(id);
    }

    async delete(id) {
        return GroupTransaction.query().deleteById(id);
    }

    async summary(group_id) {
        const rows = await GroupTransaction.query()
            .where("group_id", group_id)
            .select("type")
            .sum("amount as total")
            .groupBy("type");

        let total_income = 0;
        let total_expense = 0;

        for (const row of rows) {
            if (row.type === "income") {
                total_income = Number(row.total) || 0;
            }

            if (row.type === "expense") {
                total_expense = Number(row.total) || 0;
            }
        }

        return {
            total_income,
            total_expense
        };
    }
}

module.exports = new GroupTransactionService();
