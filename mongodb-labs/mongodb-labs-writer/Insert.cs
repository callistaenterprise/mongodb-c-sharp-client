using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.GridFS;
using MongoDB.Driver.Linq;
using System.Threading;
using mongodb_labs_writer.Properties;

namespace mongodb_labs
{
    class Insert
    {
        private static readonly Logger logger =  new Logger();
        private static Settings settings = Settings.Default;
        private static MongoHelper helper = new MongoHelper(settings.MongoDbUrl, settings.ReplicaSet, settings.Database);
        private static MongoCollection<BsonDocument> collection = helper.GetCollection("messages");

        private const int maxRetryInsertCount  = 5;
        private const int retryInsertTimeMs    = 1000;
        private const int waitBetweenInsertsMs = 1000;

        static void Main(string[] args)
        {
            var i = getMaxValue(collection);
            logger.Debug("Found maxvalue: " + i);

            while (true)
            {
                BsonDocument doc = new BsonDocument { { "attr1", ++i } };
                try
                {
                    doInsertWithRetries(collection, doc);
                }
                catch (Exception ex)
                {
                    logger.Error("Failed to insert doc " + doc + ", error: " + ex);
                }
                sleep(waitBetweenInsertsMs);
            }
        }

        private static void doInsertWithRetries(MongoCollection<BsonDocument> collection, BsonDocument doc)
        {
            int retries = 0;
            Boolean ok = false;
 
            while (!ok) {
                try
                {
                    doInsert(collection, doc);
                    ok = true;
                }
                catch (Exception ex)
                {
                    retries++;

                    if (retries < maxRetryInsertCount)
                    {
                        logger.Warning("Insert failed, retrying in " + retryInsertTimeMs + "ms..., exception: " + ex.Message);
                        sleep(retryInsertTimeMs);
                        logger.Info("- Retry #" + retries + " of " + maxRetryInsertCount + " ...");
                        // Actually required?
                        // collection = getNewCollection();

                    } else {
                        logger.Warning("Insert reached max-retry-count, giving up..." + ex);
                        throw ex;
                    }
                }
            }
        }

        static private void doInsert(MongoCollection<BsonDocument> collection, BsonDocument doc)
        {
            logger.Debug("Trying to insert: " + doc);
            collection.Insert(doc);
            logger.Debug("Insert ok to " + collection.Database.Server.Primary.Address);
        }

        static private long getMaxValue(MongoCollection collection)
        {
            try
            {
                //  db.messages.find().sort({"attr1":-1}).limit(1)
                return collection.FindAllAs<BsonDocument>().SetSortOrder(SortBy.Descending("attr1")).First()["attr1"].AsInt64;
            }
            catch (Exception ex)
            {
                logger.Warning("Counting failed, ex: " + ex.Message);
                return -1;
            }
        }

        static private void sleep(int ms)
        {
            Thread.Sleep(ms);
        }


        /*
        private static void doInsertWithOneRetry(MongoCollection<BsonDocument> collection, BsonDocument doc)
        {
            try
            {
                doInsert(collection, doc);

            }
            catch (Exception ex1)
            {
                logger.Warning("Insert failed, retrying in " + retryInsertTimeMs + "ms..., exception: " + ex1.Message);
                sleep(retryInsertTimeMs);
                logger.Info("- Retrying...");

                try
                {
                    collection = getNewCollection();
                    doInsert(collection, doc);

                }
                catch (Exception ex2)
                {
                    logger.Error("Insert retry also failed" + ex2);
                    throw ex2;
                }
            }
        }
        */
    }
}
