using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Text;
using System.Threading.Tasks;

namespace cosmos_db_mongodb_multithread
{
    public class Constrants
    {
        public const string cosmos_mongo_insert = "cosmos_mongo_insert";        
    }

    class Program
    {
        private static int _client;
        private static int _times;        

        private static Dictionary<string, List<Thread>> _threads = new Dictionary<string, List<Thread>>();
        static void Main(string[] args)
        {
            Console.WriteLine("thread:");
            //_client = int.Parse(Console.ReadLine());
            _client = 10;
            Console.WriteLine("times:");
            //_times = int.Parse(Console.ReadLine());
            _times = 1000;

            Initialize();
            Console.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + ", threads starting ...");
            Run(Constrants.cosmos_mongo_insert);
            
        }

        private static void Initialize()
        {
            Console.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + ", initializing ...");
            _threads.Add(Constrants.cosmos_mongo_insert, new List<Thread>());

            for (int i = 1; i <= _client; i++)
            {
                Console.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + ", add thread #{0}", i.ToString());
                _threads[Constrants.cosmos_mongo_insert].Add(new Thread(() =>
                {
                    var cosmos_mongo = new cosmos_mongo();                    
                    //cosmos_mongo.Insert(_times);
                    cosmos_mongo.InsertMany(_times);
                }));
            }
        }

        private static void Run(string key)
        {
            Console.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + $", {key} started.");
            foreach (var thread in _threads[key])
            {
                thread.Start();
            }
            
        }

    }
}
