using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.GridFS;
using MongoDB.Driver.Linq;

namespace mongodb_labs
{
    public class MongoHelper
    {
        private static readonly Logger logger = new Logger();
        private MongoClient client = null;
        private MongoDatabase database = null;

        public MongoHelper(String mongodDbUrl, String replicaSet, String databaseName)
        {
            client = GetNewMongoClient(mongodDbUrl, replicaSet);
            database = GetDb(databaseName);
        }

        public MongoCollection<BsonDocument> GetCollection(String name)
        {
            return database.GetCollection<BsonDocument>(name);
        }

        // Summary:
        //     Create a Mongo Client object.
        //
        // Parameters:
        //   mongoDburl:
        //     E.g.: "mongodb://DB1,DB2,DB3"
        //   
        //   replicaSet:
        //     E.g.: "RS1"
        //
        private MongoClient GetNewMongoClient(String mongodDbUrl, String replicaSet)
        {
            // Create settings object based on the connect string
            MongoClientSettings mcs = MongoClientSettings.FromUrl(new MongoUrl(mongodDbUrl));
            mcs.ReplicaSetName = replicaSet;

            // Define connect and request timeouts
            mcs.ConnectTimeout = new TimeSpan(0, 0, 1);
            mcs.SocketTimeout  = new TimeSpan(0, 0, 3);
            // mcs.MaxConnectionIdleTime = new TimeSpan(0, 0, 1); // Usage???
            // mcs.MaxConnectionLifeTime = new TimeSpan(0, 0, 3); // Usage???

            // Setup the read preference
            // mcs.ReadPreference = ReadPreference.Primary;
             mcs.ReadPreference = ReadPreference.Secondary;
//            mcs.ReadPreference = ReadPreference.PrimaryPreferred;

            // Setup the write concern and the write timeout
            mcs.WriteConcern = new WriteConcern();
            mcs.WriteConcern.W = 2; // Wait for data written on two nodes, e.g. cluster safe in a three node cluster
            mcs.WriteConcern.WTimeout = new TimeSpan(0, 0, 3);

            // Create and return the client object
            MongoClient client = new MongoClient(mcs);
            logger.Debug("Options: " + client.Settings.ToString().Replace(';', '\n'));
            return client;
        }

        private MongoDatabase GetDb(String name)
        {
            return client.GetServer().GetDatabase(name);
        }

    }
}
