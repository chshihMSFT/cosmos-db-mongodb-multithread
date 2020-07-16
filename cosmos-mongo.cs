using System;
using System.Configuration;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace cosmos_db_mongodb_multithread
{
    class cosmos_mongo
    {
        private MongoUrl mongoUrl;
        private MongoClientSettings mongoClientSettings;
        private MongoClient mongoClient;
        private IMongoDatabase mongoDatabase;
        private IMongoCollection<BsonDocument> mongoCollection;
        public cosmos_mongo()
        {
            string mongoConnectionStringRaw = ConfigurationManager.AppSettings["connectionstring"];

            if (string.IsNullOrWhiteSpace(mongoConnectionStringRaw))
            {
                Console.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + ", ERROR:connectionstring is empty.");
                return;
            }
            try
            {
                mongoUrl = new MongoUrl(mongoConnectionStringRaw);
                mongoClientSettings = MongoClientSettings.FromUrl(mongoUrl);
                mongoClientSettings.ReadPreference = ReadPreference.Nearest;
                mongoClient = new MongoClient(mongoClientSettings);
                String databasename = ConfigurationManager.AppSettings["database"];
                String collectionname = ConfigurationManager.AppSettings["collection"];
                mongoDatabase = mongoClient.GetDatabase(databasename);
                mongoCollection = mongoDatabase.GetCollection<BsonDocument>(collectionname);

                //Get collection's information                              
                BsonDocument command = new BsonDocument(
                    new BsonElement("customAction", "GetCollection"),
                    new BsonElement("collection", collectionname)
                );
                var getresult = mongoDatabase.RunCommand<BsonDocument>(command);
                //Console.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + ", " + databasename + "/" + collectionname + " connected");
                //Console.WriteLine(getresult);
            }
            catch (Exception e)
            {
                Console.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + ", " + e.Message);
            }
        }

        public void Insert(int _times)
        {
            Stopwatch stopWatch = new Stopwatch();
            BsonDocument template = new BsonDocument(
                new BsonElement("_myid", new BsonString(Guid.NewGuid().ToString())),
                new BsonElement("writtenTime", new BsonDateTime(DateTime.UtcNow)),
                new BsonElement("mypartition", new BsonString(Guid.NewGuid().ToString().Substring(0, 2))),
                #region document payload
                new BsonElement("writtenText01", new BsonString(Guid.NewGuid().ToString())),
                new BsonElement("writtenText02", new BsonString(Guid.NewGuid().ToString()))
                #endregion
            );

            try
            {
                stopWatch.Start();
                for (int i = 0; i < _times; ++i)
                {
                    BsonDocument toInsert = (BsonDocument)template.DeepClone();
                    mongoCollection.InsertOne(toInsert);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + $", thread completed in {stopWatch.ElapsedMilliseconds} milliseconds.");
        }

        public void InsertMany(int _times)
        {
            Stopwatch stopWatch = new Stopwatch();
            BsonDocument template = new BsonDocument(
                new BsonElement("_myid", new BsonString(Guid.NewGuid().ToString())),
                new BsonElement("writtenTime", new BsonDateTime(DateTime.UtcNow)),
                new BsonElement("mypartition", new BsonString(Guid.NewGuid().ToString().Substring(0, 2))),
                #region document payload
                new BsonElement("writtenText01", new BsonString(Guid.NewGuid().ToString())),
                new BsonElement("writtenText02", new BsonString(Guid.NewGuid().ToString()))
                #endregion
            );

            var listWrites = new List<BsonDocument>();
            try
            {
                stopWatch.Start();
                for (int i = 0; i < _times; ++i)
                {
                    BsonDocument toInsert = (BsonDocument)template.DeepClone();
                    toInsert.Set("_myid", Guid.NewGuid().ToString());
                    toInsert.Set("mypartition", Guid.NewGuid().ToString().Substring(0, 2));                    
                    listWrites.Add(new BsonDocument(toInsert));
                }
                Console.WriteLine("each thread starting to insert " + listWrites.Count.ToString() + " documents");
                mongoCollection.InsertMany(listWrites);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + $", thread completed in {stopWatch.ElapsedMilliseconds} milliseconds.");
        }


    }
}
