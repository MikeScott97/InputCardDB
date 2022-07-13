using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace InputTesting
{
#nullable enable
    class Program
    {
       static string connectionString = @"Persist Security Info=False;Trusted_Connection=True;  
    database = Cards; server = (local)";
        static void Main(string[] args)
        {
            GetCard();

        }

        public static async void GetCard()
        {
            string data = await File.ReadAllTextAsync("C:\\Users\\Temp\\Downloads\\test.json");
            List<CardDataStore> carddata = JsonConvert.DeserializeObject<List<CardDataStore>>(data);
            
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                int lastInput;
                var checkCard = "SELECT ID FROM Card_Main WHERE Name = @Name";
                var sqlMain = "INSERT INTO Card_Main (Name, CMC, Layout, Mana_Cost, Oracle_Text, Power, Toughness, Reserved) Values(@Name, @CMC, @Layout, @Mana_Cost, @Oracle_Text, @Power, @Toughness, @Reserved); SELECT CAST(scope_identity() AS int)";

                //inputs each json line into the DB
                foreach (CardDataStore card in carddata)
                {
                    //sets the card_main values
                    using (SqlCommand writeCard = new SqlCommand(sqlMain, connection))
                    {
                        writeCard.Parameters.AddWithValue("@Name", card.Name);
                        writeCard.Parameters.AddWithValue("@CMC", card.CMC);
                        writeCard.Parameters.AddWithValue("@Layout", card.Layout);
                        writeCard.Parameters.AddWithValue("@Mana_Cost", card.Mana_Cost);
                        writeCard.Parameters.AddWithValue("@Oracle_Text", card.Oracle_Text);
                        writeCard.Parameters.AddWithValue("@Power", card.Power);
                        writeCard.Parameters.AddWithValue("@Toughness", card.Toughness);
                        writeCard.Parameters.AddWithValue("@Reserved", card.Reserved);
                        connection.Open();
                        try
                        {
                            //try to input into database
                            lastInput = (int)writeCard.ExecuteScalar();
                            InsertTypes(card.type_line, lastInput);
                         }
                        catch
                        {
                            //if entry exists get ID 
                            Console.WriteLine("Duplicate entry");
                            SqlCommand check = new SqlCommand(checkCard, connection);
                            check.Parameters.AddWithValue("@Name", card.Name);
                            lastInput = (int)check.ExecuteScalar();
                        }
                        connection.Close();
                    }
                    


                }   
            }
        }
        //function to deal with inserting into the Card_Type Database
        public static async void InsertTypes(string cardTypes, int cardID)
        {

            //put each card type into an array
            string[] cardTypeArr = Array.Empty<string>();
            cardTypeArr = cardTypes.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            int index = 0, typeID;

            //sql commands
            //search card types
            var checkType = "SELECT ID FROM Card_Types WHERE Name = @Name";
            //insert card type
            var insertType = "INSERT INTO Card_Types VALUES (@Name, @Type_Version); SELECT CAST(scope_identity() AS int)";
            //add relation to lookup table
            var fkLookupInsert = "INSERT INTO Card_Types_Lookup VALUES(@Card_ID, @Types_ID)";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                //set null for normal types and setup both insert commands prior to the while loop
                SqlCommand card_TypesInsert = new SqlCommand(insertType, connection);
                card_TypesInsert.Parameters.AddWithValue("@Type_Version", DBNull.Value);
                SqlCommand card_Type_LookupInsert = new SqlCommand(fkLookupInsert, connection);

                //while loop to go through each type
                do
                {
                    //trigger subtype inserts into db instead of regular types
                    if (cardTypeArr[index].Contains('—'))
                    {
                        card_TypesInsert.Parameters.Clear();
                        card_TypesInsert.Parameters.AddWithValue("@Type_Version", "Subtype");
                        index++;
                    }
                    //add the type name to the select query
                    SqlCommand check = new SqlCommand(checkType, connection);
                    check.Parameters.AddWithValue("@Name", cardTypeArr[index]);

                    connection.Open();
                    try
                    {
                        //check for if the type is in the db, return ID if it exists
                        typeID = (int)check.ExecuteScalar();
                    }
                    catch
                    {
                        //if nothing is returned create the table entry
                        Console.WriteLine("New Type");
                        card_TypesInsert.Parameters.AddWithValue("@Name", cardTypeArr[index]);
                        typeID = (int)card_TypesInsert.ExecuteScalar();
                    }
                    //add the cardID and typeID to the card_type_lookup table
                    card_Type_LookupInsert.Parameters.Clear();
                    card_Type_LookupInsert.Parameters.AddWithValue("@Card_ID", cardID);
                    card_Type_LookupInsert.Parameters.AddWithValue("@Types_ID", typeID);
                    try
                    {
                        card_Type_LookupInsert.ExecuteNonQuery();
                    }
                    catch
                    {
                        Console.WriteLine("duplicate " + cardTypeArr[index]);
                    }
                    connection.Close();

                    index++;
                } while (index < cardTypeArr.Length);
            }
        }
    }

    public class CardDataStore
    {
        //card_main columns
        public string Name { get; set; }
        public float CMC { get; set; }
        public string Layout { get; set; }
        public bool Reserved { get; set; }

        public string? Mana_Cost { get; set; }
        public string? Oracle_Text { get; set; }
        public string? Power { get; set; }
        public string? Toughness { get; set; }

        //card_version columns
        public string? Artist { get; set; }
        public string Border_Color { get; set; }
        public int? Collector_Number { get; set; }
        public string? Flavor_Name { get; set; }
        public string? Flavor_Text { get; set; }
        public bool Highres_Image { get; set; }
        public string Image_Status { get; set; }

        //Image_URIs columns
        //dictionary of image urls in the following order: small, normal, large, png, art_crop,  border_crop
        public IDictionary Image_URIs { get; set; }

        //Magic_Sets columns
        public string Set_Name { get; set; }
        public string? Set_URI { get; set; }
        public string Set { get; set; }
        public string? MTGO_Code { get; set; }

        //Service_IDs columns
        public int? Arena_ID { get; set; }
        public string Lang { get; set; }
        public int? MTGO_ID { get; set; }
        public int? MTGO_Foil_ID { get; set; }
        public int? TCGPlayer_ID { get; set; }
        public int? TCGPlayer_Etched_ID { get; set; }
        public int? Cardmarket_ID { get; set; }
        public string? Oracle_ID { get; set; }
        
        //bind to Scryfall_ID
        public string ID { get; set; }

        //Legalities columns
        //dictionary of legalities, relevant order is: standard, historic, pioneer, modern, legacy, pauper, vintage, commander, brawl, historicbrawl, alchemy
        public IDictionary legalities { get; set; }

        //card colours/identitys for colour table
        public char[]? colors { get; set; }
        public char[]? color_identity { get; set; }

        //different multiverse IDs
        public string[]? multiverse_IDs { get; set; }

        //card data prices for prices table
        //assume all price data is stale when imported
        public IDictionary prices { get; set; }

        //get the type for splitting later
        public string type_line { get; set; }
    }


}
