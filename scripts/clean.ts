import { createKafka } from '../src/lib/kafka.js';
import { deleteLabTopics } from '../src/lib/topics.js';

const count = await deleteLabTopics(createKafka());
console.log(`${count} adet lab.* topic silindi.`);
