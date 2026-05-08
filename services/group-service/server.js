// Load .env.dev khi dev local, Railway tự inject env vars
if (process.env.NODE_ENV !== 'production' && !process.env.RAILWAY_ENVIRONMENT) {
  require('dotenv').config({ path: '.env.dev' });
}
const app = require('./src/app');
const outboxWorker = require('./src/workers/outbox_worker');

const PORT = process.env.PORT || 5003;
app.listen(PORT, () => {
  console.log(`🚀 Group Service running on port ${PORT}`);
  // Start sau khi server đã listen — đảm bảo DB migration đã chạy xong
  outboxWorker.start();
});
