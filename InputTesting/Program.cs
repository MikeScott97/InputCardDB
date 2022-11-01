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
            var cardMethod = Task.Run(GetCard);
            Task.WaitAll(cardMethod);


        }

        public static async Task GetCard()
        {
            string data = await File.ReadAllTextAsync("C:\\Users\\temp\\Downloads\\default-cards-20221030090532.json");
            List<CardDataStore> carddata = JsonConvert.DeserializeObject<List<CardDataStore>>(data);

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                int lastInput, cardVersionID;
                var checkCard = "SELECT ID FROM Card_Main WHERE Name = @Name";
                var sqlMain = "INSERT INTO Card_Main Values(@Name, @CMC, @Layout, @Mana_Cost, @Oracle_Text, @Power, @Toughness, @Reserved, @Oracle_ID); SELECT CAST(scope_identity() AS int)";
                //(Name, CMC, Layout, Mana_Cost, Oracle_Text, Power, Toughness, Reserved)
                //inputs each json line into the DB
                foreach (CardDataStore card in carddata)
                {
                    switch(card.Layout)
                    {
                        case "token":
                        case "art_series":
                        case "emblem":
                            continue;
                        case "transform":
                        case "modal_dfc":
                            card.Mana_Cost = card.Card_Faces[0].Mana_Cost;
                            card.Oracle_Text = card.Card_Faces[0].Oracle_Text;
                            card.Power = card.Card_Faces[0].Power;
                            card.Toughness = card.Card_Faces[0].Toughness;
                            card.Flavor_Name = card.Card_Faces[0].Flavor_Name;
                            card.Flavor_Text = card.Card_Faces[0].Flavor_Text;
                            break;
                        case "reversible_card":
                            card.Mana_Cost = card.Card_Faces[0].Mana_Cost;
                            card.Oracle_Text = card.Card_Faces[0].Oracle_Text;
                            card.Power = card.Card_Faces[0].Power;
                            card.Toughness = card.Card_Faces[0].Toughness;
                            card.Flavor_Name = card.Card_Faces[0].Flavor_Name;
                            card.Flavor_Text = card.Card_Faces[0].Flavor_Text;
                            card.type_line = card.Card_Faces[0].type_line;
                            break;
                    }


                    //sets the card_main values
                    using (SqlCommand writeCard = new SqlCommand(sqlMain, connection))
                    {
                        writeCard.Parameters.AddWithValue("@Name", card.Name);
                        writeCard.Parameters.AddWithValue("@CMC", card.CMC);
                        writeCard.Parameters.AddWithValue("@Layout", card.Layout);
                        writeCard.Parameters.AddWithValue("@Mana_Cost", card.Mana_Cost ?? (object)DBNull.Value);
                        writeCard.Parameters.AddWithValue("@Oracle_Text", card.Oracle_Text ?? (object)DBNull.Value);
                        writeCard.Parameters.AddWithValue("@Power", card.Power ?? (object)DBNull.Value);
                        writeCard.Parameters.AddWithValue("@Toughness", card.Toughness ?? (object)DBNull.Value);
                        writeCard.Parameters.AddWithValue("@Reserved", card.Reserved);
                        writeCard.Parameters.AddWithValue("@Oracle_ID", card.Oracle_ID ?? (object)DBNull.Value);

                        Console.WriteLine(card.Name);
                        connection.Open();
                        try
                        {
                            //try to input into database
                            lastInput = (int)writeCard.ExecuteScalar();
                            Console.WriteLine("main input successful");

                            InsertTypes(card.type_line, lastInput);
                            Console.WriteLine("type input successful");

                            switch(card.Layout)
                            {
                                case "transform":
                                case "modal_dfc":
                                case "reversible_card":
                                InsertColours(card.Card_Faces[0].colors, lastInput, false);
                                    break;
                                default:
                                InsertColours(card.colors, lastInput, false);
                                    break;
                            }

                            Console.WriteLine("colour input successful");
                            InsertColours(card.color_identity, lastInput, true);
                            Console.WriteLine("identity input successful");
                            InsertLegalities(card.legalities, lastInput);
                            Console.WriteLine("legality input successful");
                        }
                        catch
                        {
                            //if entry exists get ID 
                            Console.WriteLine("Duplicate entry");
                            SqlCommand check = new SqlCommand(checkCard, connection);
                            check.Parameters.AddWithValue("@Name", card.Name);
                            lastInput = (int)check.ExecuteScalar();
                        }
                        cardVersionID = await InsertVersion(card, lastInput);
                        connection.Close();
                    }
                    Console.WriteLine(card.Name + "Complete");

                }
            }
        }
        //function to deal with inserting into the Card_Type Database
        public static async void InsertTypes(string cardTypes, int cardID)
        {

            //put each card type into a list
            List<string> cardTypeArr = new List<string>();
            cardTypeArr = cardTypes.Split(' ', StringSplitOptions.RemoveEmptyEntries).ToList();
            
            int index = 0, typeID;
            bool subTrigger = false;
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
                    switch (cardTypeArr[index])
                    {
                        //used to change legacy summon card to typeline "creature — subtype"
                        case "Summon":
                            cardTypeArr[index] = "Creature";
                            cardTypeArr.Add(cardTypeArr[index+1]);
                            cardTypeArr[index + 1] = "—";

                            break;
                        case "Interrupt":
                            cardTypeArr[index] = "Instant";
                            break;
                        default:
                            break;
                    }


                    if (cardTypeArr[index].Contains('/'))
                    {
                        break;

                    }

                    //trigger subtype inserts into db instead of regular types
                    if (cardTypeArr[index].Contains('—'))
                    {
                        index++;
                        subTrigger = true;
                    }
                    connection.Open();
                    //add the type name to the select query
                    using (SqlCommand check = new(checkType, connection))
                    {
                        check.Parameters.AddWithValue("@Name", cardTypeArr[index]);


                        try
                        {
                            //check for if the type is in the db, return ID if it exists
                            typeID = (int)check.ExecuteScalar();
                        }
                        catch
                        {
                            //if nothing is returned create the table entry
                            Console.WriteLine("New Type");
                            if(subTrigger == true)
                            {
                                card_TypesInsert.Parameters.Clear();
                                card_TypesInsert.Parameters.AddWithValue("@Type_Version", "Subtype");
                            }

                            card_TypesInsert.Parameters.AddWithValue("@Name", cardTypeArr[index]);
                            typeID = (int)card_TypesInsert.ExecuteScalar();
                            card_TypesInsert.Parameters.Clear();
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

                    }

                    index++;

                } while (index < cardTypeArr.Count-1);
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

                if(colours == null || colours.Length == 0)
                {
                    colours = new char[] { 'C' };
                }

                foreach (char colour in colours)
                {
                    connection.Open();
                    check.Parameters.AddWithValue("@Abbreviation", colour);
                    if (identity == false)
                    {
                        insertColourLookup.Parameters.AddWithValue("@cardID", cardID);
                        insertColourLookup.Parameters.AddWithValue("@colourID", check.ExecuteScalar());

                        insertColourLookup.ExecuteNonQuery();
                        insertColourLookup.Parameters.Clear();
                    }
                    else
                    {
                        insertIdentityLookup.Parameters.AddWithValue("@cardID", cardID);
                        insertIdentityLookup.Parameters.AddWithValue("@colourID", check.ExecuteScalar());

                        insertIdentityLookup.ExecuteNonQuery();
                        insertIdentityLookup.Parameters.Clear();

                    }
                    check.Parameters.Clear();
                    connection.Close();

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
                    string parseLegal = "@" + reader[0];

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
            var insertVersion = "INSERT INTO Card_Version Values(@Artist, @Border_Colour, @Collector_Number, @Flavour_Name, @Flavour_Text, @HighRes_Image, @Image_Status, @Main_ID, @Set_ID); SELECT CAST(scope_identity() AS int)";
            var checkSet = "SELECT ID FROM Magic_Set WHERE Code = @SetCode";
            var checkVersion = "SELECT ID FROM Card_Version WHERE Collector_Number = @Collector_Number AND Set_ID = @Set_ID AND Main_ID = @Main_ID";
            int setID, versionID;

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand selectSet = new SqlCommand(checkSet, connection);
                SqlCommand insVersion = new SqlCommand(insertVersion, connection);
                SqlCommand chkVersion = new SqlCommand(checkVersion, connection);

                selectSet.Parameters.AddWithValue("@SetCode", CardData.Set);
                try
                {
                    connection.Open();
                    setID = (int)selectSet.ExecuteScalar();
                }
                catch
                {
                    setID = await InsertSet(CardData.Set);
                }
                connection.Close();


                insVersion.Parameters.AddWithValue("@Artist", CardData.Artist ?? (object)DBNull.Value);
                insVersion.Parameters.AddWithValue("@Border_Colour", CardData.Border_Color);
                insVersion.Parameters.AddWithValue("@Collector_Number", CardData.Collector_Number);
                insVersion.Parameters.AddWithValue("@Flavour_Name", CardData.Flavor_Name ?? (object)DBNull.Value);
                insVersion.Parameters.AddWithValue("@Flavour_Text", CardData.Flavor_Text ?? (object)DBNull.Value);
                insVersion.Parameters.AddWithValue("@HighRes_Image", CardData.Highres_Image);
                insVersion.Parameters.AddWithValue("@Image_Status", CardData.Image_Status);
                insVersion.Parameters.AddWithValue("@Main_ID", cardID);
                insVersion.Parameters.AddWithValue("@Set_ID", setID);
                try
                {
                    connection.Open();
                    versionID = (int)insVersion.ExecuteScalar();
                    connection.Close();
                    switch(CardData.Image_URIs == null)
                    {
                        case true:
                            InsertImages(versionID, CardData.Card_Faces[0].Image_URIs, 1);
                            InsertImages(versionID, CardData.Card_Faces[1].Image_URIs, 2);
                            break;

                        default:
                            InsertImages(versionID, CardData.Image_URIs, 0);
                            break;
                    }

                    InsertPrices(versionID, CardData.prices);
                    InsertServiceIDs(versionID, CardData);
                    Console.WriteLine("Version input successful");
                    
                }
                catch
                {
                    connection.Close();
                    chkVersion.Parameters.AddWithValue("@Collector_Number", CardData.Collector_Number);
                    chkVersion.Parameters.AddWithValue("@Main_ID", cardID);
                    chkVersion.Parameters.AddWithValue("@Set_ID", setID);
                    connection.Open();
                    versionID = (int)chkVersion.ExecuteScalar();
                    connection.Close();
                    Console.WriteLine("Version already exists");
                }

                return versionID;   

            }
        }

        public static async Task<int> InsertSet(string setCode)
        {
            int setID;
            var insertSet = "INSERT INTO Magic_Set VALUES(@Name, @Icon, @Code, @Card_Count, @TCGPlayer_ID, @Scryfall_ID); SELECT CAST(scope_identity() AS int)";
            //scryfall generic URI for sets with the setcode added onto it
            string uri = "https://api.scryfall.com/sets/" + setCode;
            //sleep the thread as scryfall limits requests to 10 per second
            await Task.Delay(100);
            using var client = new HttpClient();
            //get request for set data
            var getSet = await client.GetStringAsync(uri);

            SetDataStore setData = JsonConvert.DeserializeObject<SetDataStore>(getSet);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand insSet = new SqlCommand(insertSet, connection);
                insSet.Parameters.AddWithValue("@Name", setData.name);
                insSet.Parameters.AddWithValue("@Icon", setData.icon_svg_uri);
                insSet.Parameters.AddWithValue("@Code", setData.code);
                insSet.Parameters.AddWithValue("@Card_Count", setData.card_count);
                insSet.Parameters.AddWithValue("@TCGPlayer_ID", setData.tcgplayer_id ?? (object)DBNull.Value);
                insSet.Parameters.AddWithValue("@Scryfall_ID", setData.id);

                connection.Open();
                setID = (int)insSet.ExecuteScalar();
                connection.Close();
                Console.WriteLine("set input successful");
                return setID;

            }
        }
        public static async void InsertImages(int versionID, Dictionary<string, string> ImageURIs, int faceID)
        {
            
            var insertImages = "INSERT INTO Image_URIs VALUES(@Card_Version_ID, @PNG, @Border_Crop, @Art_Crop, @Large, @Normal, @Small, @Card_Face)";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand insImages = new SqlCommand(insertImages, connection);

                insImages.Parameters.AddWithValue("@Card_Version_ID", versionID);
                insImages.Parameters.AddWithValue("@PNG", ImageURIs == null ? (object)DBNull.Value : ImageURIs["png"]);
                insImages.Parameters.AddWithValue("@Border_Crop", ImageURIs == null ? (object)DBNull.Value : ImageURIs["border_crop"]);
                insImages.Parameters.AddWithValue("@Art_Crop", ImageURIs == null ? (object)DBNull.Value : ImageURIs["art_crop"]);
                insImages.Parameters.AddWithValue("@Large", ImageURIs == null ? (object)DBNull.Value : ImageURIs["large"]);
                insImages.Parameters.AddWithValue("@Normal", ImageURIs == null ? (object)DBNull.Value : ImageURIs["normal"]);
                insImages.Parameters.AddWithValue("@Small", ImageURIs == null ? (object)DBNull.Value : ImageURIs["small"]);
                insImages.Parameters.AddWithValue("@Card_Face", faceID);
                connection.Open();
                insImages.ExecuteNonQuery();
                connection.Close();

                Console.WriteLine("image input successful");
            }
        }
        public static async void InsertPrices(int versionID, Dictionary<string, string> prices)
        {
            var insertPrices = "INSERT INTO Prices VALUES(@Card_version_ID, @usd, @usd_foil, @usd_etched, @eur, @eur_foil, @tix)";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand insPrices = new SqlCommand(insertPrices, connection);

                insPrices.Parameters.AddWithValue("@Card_Version_ID", versionID);
                insPrices.Parameters.AddWithValue("@usd", prices["usd"] ?? (object)DBNull.Value);
                insPrices.Parameters.AddWithValue("@usd_foil", prices["usd_foil"] ?? (object)DBNull.Value);
                insPrices.Parameters.AddWithValue("@usd_etched", prices["usd_etched"] ?? (object)DBNull.Value);
                insPrices.Parameters.AddWithValue("@eur", prices["eur"] ?? (object)DBNull.Value);
                insPrices.Parameters.AddWithValue("@eur_foil", prices["eur_foil"] ?? (object)DBNull.Value);
                insPrices.Parameters.AddWithValue("@tix", prices["tix"] ?? (object)DBNull.Value);

                connection.Open();
                insPrices.ExecuteNonQuery();
                connection.Close();
                Console.WriteLine("price input successful");
            }
        }
        public static async void InsertServiceIDs(int versionID, CardDataStore card)
        {
            int serviceID;
            var insertServiceIDs = "INSERT INTO Service_IDs VALUES(@Version_ID, @Arena_ID, @Scryfall_ID, @Lang, @MTGO_ID, @MTGO_Foil_ID, @TCGPlayer_ID, @TCGPlayer_Etched_ID, @Cardmarket_ID);SELECT CAST(scope_identity() AS int)";
            var insertMultiverseID = "INSERT INTO Multiverse_ID VALUES(@Service_ID, @Multiverse_ID)";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand insServiceIDs = new SqlCommand(insertServiceIDs, connection);
                SqlCommand insMultiverseID = new SqlCommand(insertMultiverseID, connection);

                insServiceIDs.Parameters.AddWithValue("@Version_ID", versionID);
                insServiceIDs.Parameters.AddWithValue("@Arena_ID", card.Arena_ID ?? (object)DBNull.Value);
                insServiceIDs.Parameters.AddWithValue("@Scryfall_ID", card.ID);
                insServiceIDs.Parameters.AddWithValue("@Lang", card.Lang);
                insServiceIDs.Parameters.AddWithValue("@MTGO_ID", card.MTGO_ID ?? (object)DBNull.Value);
                insServiceIDs.Parameters.AddWithValue("@MTGO_Foil_ID", card.MTGO_Foil_ID ?? (object)DBNull.Value);
                insServiceIDs.Parameters.AddWithValue("@TCGPlayer_ID", card.TCGPlayer_ID ?? (object)DBNull.Value);
                insServiceIDs.Parameters.AddWithValue("@TCGPlayer_Etched_ID", card.TCGPlayer_Etched_ID ?? (object)DBNull.Value);
                insServiceIDs.Parameters.AddWithValue("@Cardmarket_ID", card.Cardmarket_ID ?? (object)DBNull.Value);

                connection.Open();
                serviceID = (int)insServiceIDs.ExecuteScalar();
                Console.WriteLine("services input successful");

                foreach (int ID in card.multiverse_IDs)
                {
                    insMultiverseID.Parameters.AddWithValue("@Service_ID", serviceID);
                    insMultiverseID.Parameters.AddWithValue("@Multiverse_ID", ID);
                    insMultiverseID.ExecuteNonQuery();

                    insMultiverseID.Parameters.Clear();

                }
                Console.WriteLine("Multiverse input successful");
                connection.Close();
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
            public string? Collector_Number { get; set; }
            public string? Flavor_Name { get; set; }
            public string? Flavor_Text { get; set; }
            public bool Highres_Image { get; set; }
            public string Image_Status { get; set; }

            //card version of transform cards
            public IList<CardFaceDataStore>? Card_Faces { get; set; }

            //Image_URIs columns
            //dictionary of image urls in the following order: small, normal, large, png, art_crop,  border_crop
            public Dictionary<string, string>? Image_URIs { get; set; }

            //Magic_Sets columns
            public string Set_Name { get; set; }
            public string? Set_URI { get; set; }
            public string Set { get; set; }

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
            public int[]? multiverse_IDs { get; set; }

            //card data prices for prices table
            //assume all price data is stale when imported
            public Dictionary<string, string> prices { get; set; }

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
            public int? tcgplayer_id { get; set; }
            public int card_count { get; set; }
            public string name { get; set; }

        }

        //object for cardfaces
        public class CardFaceDataStore
        {
            public string Name { get; set; }
            public string Mana_Cost { get; set; }
            public char[]? colors { get; set; }
            public string? Oracle_Text { get; set; }
            public string? Power { get; set; }
            public string? Toughness { get; set; }
            public Dictionary<string, string>? Image_URIs { get; set; }
            public string? Flavor_Name { get; set; }
            public string? Flavor_Text { get; set; }
            public string type_line { get; set; }

        }

    }
}
