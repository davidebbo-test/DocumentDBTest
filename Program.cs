using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;

namespace DocumentDBTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string endpoint = ConfigurationManager.AppSettings["endpoint"];
            string authKey = ConfigurationManager.AppSettings["authKey"];

            Test(endpoint, authKey).Wait();
        }

        static async Task Test(string endpoint, string authKey)
        {
            using (var client = new DocumentClient(new Uri(endpoint), authKey))
            {
                var database = await ReadOrCreateDatabase(client, "DavidsDemoDB");

                var collection = new DocumentCollection { Id = "Families" };
                collection = await client.CreateDocumentCollectionAsync(database.SelfLink, collection);

                //DocumentDB supports strongly typed POCO objects and also dynamic objects
                dynamic andersonFamily = JsonConvert.DeserializeObject(File.ReadAllText(@"..\..\Data\AndersonFamily.json"));
                dynamic wakefieldFamily = JsonConvert.DeserializeObject(File.ReadAllText(@"..\..\Data\WakefieldFamily.json"));

                //persist the documents in DocumentDB
                await client.CreateDocumentAsync(collection.SelfLink, andersonFamily);
                await client.CreateDocumentAsync(collection.SelfLink, wakefieldFamily);

                //very simple query returning the full JSON document matching a simple WHERE clause
                var query = client.CreateDocumentQuery(collection.SelfLink, "SELECT * FROM Families f WHERE f.id = 'AndersenFamily'");
                var family = query.AsEnumerable().FirstOrDefault();

                Console.WriteLine("The Anderson family have the following pets:");
                foreach (var pet in family.pets)
                {
                    Console.WriteLine(pet.givenName);
                }

                //select JUST the child record out of the Family record where the child's gender is male
                query = client.CreateDocumentQuery(collection.DocumentsLink, "SELECT * FROM c IN Families.children WHERE c.gender='male'");
                var child = query.AsEnumerable().FirstOrDefault();

                //Console.WriteLine("The Andersons have a son named {0} in grade {1} ", child.firstName, child.grade);

                //cleanup test database
                await client.DeleteDatabaseAsync(database.SelfLink);
            }
        }

        private static async Task<Database> ReadOrCreateDatabase(DocumentClient client, string databaseId)
        {
            var databases = client.CreateDatabaseQuery()
                            .Where(db => db.Id == databaseId).ToArray();

            if (databases.Any())
            {
                return databases.First();
            }

            Database database = new Database { Id = databaseId };
            return await client.CreateDatabaseAsync(database);
        }
    }
}
