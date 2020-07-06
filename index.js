const express = require('express');
const { MongoClient } = require('mongodb')

let db;

const connect = () => new Promise((resolve, reject) => {
	if (db) return resolve(db)
	const client = new MongoClient('mongodb://10.0.3.183:27017', { useNewUrlParser: true })
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
		.find({})
		.project({ _id: 1 })
		.sort({ _id: -1 })
		.limit(1)
		.toArray();
	res.json(posts);
});

app.listen(5000, () => {
	console.log('App Listening');
})
