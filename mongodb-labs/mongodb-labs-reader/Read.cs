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

using mongodb_labs_reader.Properties;

namespace mongodb_labs
{
    class Read
    {
        private static readonly Logger logger =  new Logger();

        private const int waitBetweenReadsMs = 500;

        private static Settings settings = Settings.Default;
        private static MongoHelper helper = new MongoHelper(settings.MongoDbUrl, settings.ReplicaSet, settings.Database);
        private static MongoCollection<BsonDocument> collection = helper.GetCollection("messages");

        static void Main(string[] args)
        {
            while (true)
            {
                var i = getMaxValue(collection);
                logger.Debug("Found max value " + i);
                sleep(waitBetweenReadsMs);
            }
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

    }
}
