// queries.js

const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function run() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    const db = client.db(dbName);
    const books = db.collection(collectionName);

    // TASK 2: BASIC CRUD
    console.log('\nTask 2: Basic CRUD');

    console.log('\nFind all books in genre "Dystopian"');
    console.log(await books.find({ genre: 'Dystopian' }).toArray());

    console.log('\nFind books published after 2000');
    console.log(await books.find({ published_year: { $gt: 2000 } }).toArray());

    console.log('\nFind books by author "George Orwell"');
    console.log(await books.find({ author: 'George Orwell' }).toArray());

    console.log('\nUpdate price of "1984" to 10.99');
    await books.updateOne({ title: '1984' }, { $set: { price: 10.99 } });

    console.log('\nDelete book titled "To Be Deleted" (if exists)');
    await books.deleteOne({ title: 'To Be Deleted' });

    // TASK 3: ADVANCED QUERIES
    console.log('\nTask 3: Advanced Queries');

    console.log('\nBooks in stock and published after 2010');
    console.log(await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray());

    console.log('\nProjection: title, author, price');
    console.log(await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray());

    console.log('\nSort by price ascending');
    console.log(await books.find().sort({ price: 1 }).toArray());

    console.log('\nSort by price descending');
    console.log(await books.find().sort({ price: -1 }).toArray());

    console.log('\nPagination: Page 1 (limit 5)');
    console.log(await books.find().limit(5).toArray());

    console.log('\nPagination: Page 2 (skip 5, limit 5)');
    console.log(await books.find().skip(5).limit(5).toArray());

    // TASK 4: AGGREGATION PIPELINE
    console.log('\nTask 4: Aggregation Pipeline');

    console.log('\nAverage price by genre');
    console.log(await books.aggregate([
      { $group: { _id: '$genre', avgPrice: { $avg: '$price' } } }
    ]).toArray());

    console.log('\nAuthor with most books');
    console.log(await books.aggregate([
      { $group: { _id: '$author', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray());

    console.log('\nBooks grouped by publication decade');
    console.log(await books.aggregate([
      {
        $group: {
          _id: { $concat: [{ $substr: [{ $toString: "$published_year" }, 0, 3] }, "0s"] },
          count: { $sum: 1 }
        }
      }
    ]).toArray());

    // TASK 5: INDEXING
    console.log('\nTask 5: Indexing');

    console.log('\nCreate index on title');
    await books.createIndex({ title: 1 });

    console.log('\nCreate compound index on author and published_year');
    await books.createIndex({ author: 1, published_year: 1 });

    console.log('\nExplain query using title index');
    console.log(await books.find({ title: '1984' }).explain());

  } catch (error) {
    console.error('❌ Error running queries:', error);
  } finally {
    await client.close();
  }
}

run();
