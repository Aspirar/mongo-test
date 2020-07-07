const express = require('express');
const compression = require('compression');
const { MongoClient } = require('mongodb')

let db;

const connect = () => new Promise((resolve, reject) => {
	if (db) return resolve(db)
	const client = new MongoClient('mongodb://localhost:27017', {
		useNewUrlParser: true,
		useUnifiedTopology: true,
		compression: ['snappy']
	})
	client.connect((err) => {
		if (err) return reject(err)
		db = client.db('rumbl')
		resolve(db)
	})
})

connect().then(() => 'Connected to mongodb')

const app = express();

app.get('/health', (req, res) => res.end('healthy'));

app.get('/test', async (req, res) => {
	const posts = await db.collection('posts')
		.find({}, { readPreference: 'secondary' })
		.project()
		.sort({ _id: -1 })
		.limit(200)
		.toArray();
	res.json(posts);
});

app.get('/write-one', async (req, res) => {
	await db.collection('test_collection').insertOne({ test: 1 });
	res.json({ success: true });
});

app.get('/write-ten', async (req, res) => {
	const docs = Array(10).fill().map((val, index) => ({ test: index }))
	await db.collection('test_collection').insertMany(docs);
	res.json({ success: true });
});

app.get('/read-one', async (req, res) => {
	const docs = await db.collection('test_collection')
		.find({}, { readPreference: 'secondaryPreferred' })
		.sort({ _id: -1 })
		.limit(1)
		.toArray();
	res.json(docs);
});

app.get('/read-ten', async (req, res) => {
	const docs = await db.collection('test_collection')
		.find({}, { readPreference: 'secondaryPreferred' })
		.sort({ _id: -1 })
		.limit(10)
		.toArray();
	res.json(docs);
});

app.get('/update-two-read-ten', async ({ req, res }) => {
	const docs = await db.collection('test_collection')
		.find({}, { readPreference: 'secondaryPreferred' })
		.sort({ _id: -1 })
		.limit(10)
		.toArray();
	const idsToUpdate = docs.slice(0, 2).map(doc => doc._id);
	await db.collection('test_collection').updateMany(
		{ _id: { $in: idsToUpdate } },
		{ $set: { test: 0 } },
	)
	res.json(docs);
});

app.get('/insert-two-read-ten', async ({ req, res }) => {
	const toInsert = Array(2).fill().map((val, index) => ({ test: index }))
	await db.collection('test_collection').insertMany(toInsert);

	const docs = await db.collection('test_collection')
		.find({}, { readPreference: 'secondaryPreferred' })
		.sort({ _id: -1 })
		.limit(10)
		.toArray();

	res.json(docs);
});

app.listen(5000, () => {
	console.log('App Listening');
});
