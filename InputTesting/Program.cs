using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Threading;
using System.Net.Http;
using System.Threading.Tasks;

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
                connection.Open();
                int lastInput, cardVersionID;
                var checkCard = "SELECT ID FROM Card_Main WHERE Name = @Name";
                var sqlMain = "INSERT INTO Card_Main Values(@Name, @CMC, @Layout, @Mana_Cost, @Oracle_Text, @Power, @Toughness, @Reserved, @Oracle_ID); SELECT CAST(scope_identity() AS int)";
                //(Name, CMC, Layout, Mana_Cost, Oracle_Text, Power, Toughness, Reserved)
                //inputs each json line into the DB
                foreach (CardDataStore card in carddata)
                {
                    cardVersionID = await InsertVersion(card, 1);
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
                        writeCard.Parameters.AddWithValue("@Oracle_ID", card.Oracle_ID);

                        try
                        {
                            //try to input into database
                            lastInput = (int)writeCard.ExecuteScalar();
                            InsertTypes(card.type_line, lastInput);
                            InsertColours(card.colors, lastInput, false);
                            InsertColours(card.color_identity, lastInput, true);
                            InsertLegalities(card.legalities, lastInput);
                            cardVersionID = await InsertVersion(card, lastInput);

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
            string[] cardTypeArr;
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
                    using (SqlCommand check = new(checkType, connection))
                    {
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
                       
                    }

                    index++;

                } while (index < cardTypeArr.Length);
            }
        }

        public static async void InsertColours(char[] colours, int cardID, bool identity)
        {
            var checkColour = "SELECT ID FROM Colours WHERE Abbreviation = @Abbreviation";
            var insertColour = "INSERT INTO Card_Colour_lookup VALUES (@cardID, @colourID)";
            var insertIdentity = "INSERT INTO Card_Identity_lookup VALUES (@cardID, @colourID)";

            using (SqlConnection connection = new SqlConnection(connectionString)) 
            {
                SqlCommand check = new SqlCommand(checkColour, connection);
                SqlCommand insertColourLookup = new SqlCommand(insertColour, connection);
                SqlCommand insertIdentityLookup = new SqlCommand(insertIdentity, connection);

                foreach (char colour in colours)
                {
                    check.Parameters.AddWithValue("@Abbreviation", colour);
                    if(identity == false)
                    {
                        insertColourLookup.Parameters.AddWithValue("@cardID", cardID);
                        insertColourLookup.Parameters.AddWithValue("@colourID", check.ExecuteScalar());

                        try
                        {
                            insertColourLookup.ExecuteNonQuery();
                        }
                        catch
                        {
                            Console.WriteLine("insert failed for " + cardID + colour);
                        }
                    }
                    else
                    {
                        insertIdentityLookup.Parameters.AddWithValue("@cardID", cardID);
                        insertIdentityLookup.Parameters.AddWithValue("@colourID", check.ExecuteScalar());

                        try
                        {
                            insertIdentityLookup.ExecuteNonQuery();
                        }
                        catch
                        {
                            Console.WriteLine("insert failed for " + cardID + colour);
                        }
                    }


                }
            }

        }

        public static async void InsertLegalities(Dictionary<string, string> legalities, int cardID)
        {
            //grabs the current columns in legalities as not all in the data will be used, but leaves room for adding legalities
            var getLegalities = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Legalities' and TABLE_SCHEMA = 'DBO' and not COLUMN_NAME in ('ID', 'card_ID')";
            var insertLegalities = "INSERT INTO Legalities Values(@Card_ID, @Standard, @Historic, @Pioneer, @Modern, @Legacy, @Pauper, @Vintage, @Commander, @Brawl, @HistoricBrawl, @Alchemy)";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlCommand insLegalities = new SqlCommand(insertLegalities, connection);
                SqlCommand grabLegalities = new SqlCommand(getLegalities, connection);
                SqlDataReader reader = grabLegalities.ExecuteReader();

                insLegalities.Parameters.AddWithValue("@Card_ID", cardID);
                while (reader.Read())
                {
                    //Console.WriteLine(reader[0]);
                    string parseLegal = "@"+reader[0];
                    
                    //values in data are returned lowercase so string needs to be lowercase
                    insLegalities.Parameters.AddWithValue(parseLegal, legalities[reader[0].ToString().ToLower()]);
                    
                }
                //close reader
                connection.Close();
                //open for insert statement
                connection.Open();
                insLegalities.ExecuteNonQuery();
                connection.Close();
   

            }
        }

        public static async Task<int> InsertVersion(CardDataStore CardData, int cardID)
        {
            var insertVersion = "INSERT INTO Card_Version Values(@Artist, @Border_Colour, @Collector_Number, @Flavour_Name, @Flavour_Text, @HighRes_Image, @Image_Status, @Main_ID, @Set_ID)";
            var checkSet = "SELECT ID FROM Magic_Set WHERE Code = @SetCode";
            int setID;

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand selectSet = new SqlCommand(checkSet, connection);
                SqlCommand insVersion = new SqlCommand(insertVersion, connection);

                selectSet.Parameters.AddWithValue("@SetCode", CardData.Set);
                try
                {
                    setID = (int)selectSet.ExecuteScalar();
                }
                catch
                {
                    setID = await InsertSet(CardData.Set);
                }

                insVersion.Parameters.AddWithValue("@Artist", CardData.Artist);
                insVersion.Parameters.AddWithValue("@Border_Colour", CardData.Border_Color);
                insVersion.Parameters.AddWithValue("@Collector_Number", CardData.Collector_Number);
                insVersion.Parameters.AddWithValue("@Flavour_Name", CardData.Flavor_Name);
                insVersion.Parameters.AddWithValue("@Flavour_Text", CardData.Flavor_Text);
                insVersion.Parameters.AddWithValue("@HighRes_Image", CardData.Highres_Image);
                insVersion.Parameters.AddWithValue("@Image_Status", CardData.Image_Status);
                insVersion.Parameters.AddWithValue("@Main_ID", cardID);
                insVersion.Parameters.AddWithValue("@Set_ID", setID);

                return (int)insVersion.ExecuteScalar();

            }
        }

        public static async Task<int> InsertSet(string setCode)
        {
            var insertSet = "INSERT INTO Magic_Set VALUES(@Name, @Icon, @Code, @Card_Count, @TCGPlayer_ID, @Scryfall_ID)";
            //scryfall generic URI for sets with the setcode added onto it
            string uri = "https://api.scryfall.com/sets/" + setCode;
            //sleep the thread as scryfall limits requests to 10 per second
            await Task.Delay(100);
            using var client = new HttpClient();

            var getSet = await client.GetStringAsync(uri);
            Console.WriteLine("Hi");
            SetDataStore setData = JsonConvert.DeserializeObject<SetDataStore>(getSet);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand insSet = new SqlCommand(insertSet, connection);
                insSet.Parameters.AddWithValue("@Name", setData.name);
                insSet.Parameters.AddWithValue("@Icon", setData.icon_svg_uri);
                insSet.Parameters.AddWithValue("@Code", setData.code);
                insSet.Parameters.AddWithValue("@Card_Count", setData.card_count);
                insSet.Parameters.AddWithValue("@TCGPlayer_ID", setData.tcgplayer_id);
                insSet.Parameters.AddWithValue("@Scryfall_ID", setData.id);

                return (int)insSet.ExecuteScalar();

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
        public string? Oracle_ID { get; set; }

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
        public Dictionary<string, string> Image_URIs { get; set; }

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

        //bind to Scryfall_ID
        public string ID { get; set; }

        //Legalities columns
        //dictionary of legalities, relevant order is: standard, historic, pioneer, modern, legacy, pauper, vintage, commander, brawl, historicbrawl, alchemy
        public Dictionary<string, string> legalities { get; set; }

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

    public class SetDataStore
    {
        //scryfall ID
        public string id { get; set; }
        public string code { get; set; }
        //icon URI
        public string icon_svg_uri { get; set; }
        public int tcgplayer_id { get; set; }
        public int card_count { get; set; }
        public string name { get; set; }

    }

}
