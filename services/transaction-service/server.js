// Load .env.dev khi dev local, Railway tự inject env vars
if (process.env.NODE_ENV !== 'production' && !process.env.RAILWAY_ENVIRONMENT) {
  require('dotenv').config({ path: '.env.dev' });
}
const app = require('./app');
const groupTransactionConsumer = require('./src/workers/group_transaction_consumer');

const PORT = process.env.PORT || 8082;

app.listen(PORT, () => {
  console.log(`🚀 Transaction Service running on port ${PORT}`);
  // Start sau khi server đã listen — đảm bảo DB migration đã chạy xong
  groupTransactionConsumer.start();
});
