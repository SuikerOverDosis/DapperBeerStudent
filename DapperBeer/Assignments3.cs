using System.Data;
using BenchmarkDotNet.Attributes;
using Dapper;
using DapperBeer.DTO;
using DapperBeer.Model;
using DapperBeer.Tests;

namespace DapperBeer;

public class Assignments3
{
    // 3.1 Question
    // Tip: Kijk in voorbeelden en sheets voor inspiratie.
    // Deze staan in de directory ExampleFromSheets/Relationships.cs. 
    // De sheets kan je vinden op: https://slides.com/jorislops/dapper/
    // Kijk niet te veel naar de voorbeelden van relaties op https://www.learndapper.com/relationships
    // Deze aanpak is niet altijd de manier de gewenst is!
    
    // 1 op 1 relatie (one-to-one relationship)
    // Een brouwmeester heeft altijd 1 adres. Haal alle brouwmeesters op en zorg ervoor dat het address gevuld is.
    // Sorteer op naam.
    // Met andere woorden een brouwmeester heeft altijd een adres (Property Address van type Address), zie de klasse Brewmaster.
    // Je kan dit doen door een JOIN te gebruiken.
    // Je zult de map functie in Query<Brewmaster, Address, Brewmaster>(sql, map: ...) moeten gebruiken om de Address property van Brewmaster te vullen.
    // Kijk in voorbeelden hoe je dit kan doen. Deze staan in de directory ExampleFromSheets/Relationships.cs.
    public static List<Brewmaster> GetAllBrouwmeestersIncludesAddress()
    {
        var connection = DbHelper.GetConnection();
        string sql = @"select brewmaster.name, brewmaster.brewmasterid, 'AddressSplit' as AddressSplit, address.city, address.street, address.addressid, address.country from brewmaster 
            join address on brewmaster.addressid=address.addressid
            order by brewmaster.name asc";
        return connection.Query<Brewmaster, Address, Brewmaster>(sql, map: (brewmaster, address) =>
        {
            brewmaster.Address = address;
            return brewmaster;
        },
            splitOn: "AddressSplit")
            .ToList();
    }

    // 3.2 Question
    // 1 op 1 relatie (one-to-one relationship)
    // Haal alle brouwmeesters op en zorg ervoor dat de brouwer (Brewer) gevuld is.
    // Sorteer op naam.
    public static List<Brewmaster> GetAllBrewmastersWithBrewery()
    {
        var connection = DbHelper.GetConnection();
        string sql = @"select brewmaster.name, brewmaster.brewmasterid, 'Split' as Split, Brewer.name, brewer.brewerid, brewer.country from Brewmaster 
            join brewer on brewmaster.brewerid=brewer.brewerid order by brewmaster.name asc";
        return connection.Query<Brewmaster, Brewer, Brewmaster>(sql, map: (brewmaster, brewer) =>{
            brewmaster.Brewer = brewer;
        return brewmaster;
    }, splitOn: "Split").ToList();
    }

    // 3.3 Question
    // 1 op 1 (0..1) (one-to-one relationship) 
    // Geef alle brouwers op en zorg ervoor dat de brouwmeester gevuld is.
    // Sorteer op brouwernaam.
    //
    // Niet alle brouwers hebben een brouwmeester.
    // Let op: gebruik het correcte type JOIN (JOIN, LEFT JOIN, RIGHT JOIN).
    // Dapper snapt niet dat het om een 1 - 0..1 relatie gaat.
    // De Query methode ziet er als volgt uit (let op het vraagteken optioneel):
    // Query<Brewer, Brewmaster?, Brewer>(sql, map: ...)
    // Wat je kan doen is in de map functie een controle toevoegen, je zou dit verwachten:
    // if (brewmaster is not null) { brewer.Brewmaster = brewmaster; }
    // !!echter dit werkt niet!!!!
    // Plaats eens een breakpoint en kijk wat er in de brewmaster variabele staat,
    // hoe moet dan je if worden?
    public static List<Brewer> GetAllBrewersIncludeBrewmaster()
    {
        var connection = DbHelper.GetConnection();
        string sql = @"select brewer.brewerid, brewer.name, brewer.country, 'Split' as Split, brewmaster.brewerid, brewmaster.name from brewer
                     left join brewmaster on brewer.brewerid=brewmaster.brewerid 
                     order by brewer.name asc";
        return connection.Query<Brewer, Brewmaster, Brewer>(sql, map: (brewer, brewmaster) =>
        {
            if (brewmaster.Name != null)
            {
                brewer.Brewmaster = brewmaster;
            }
            return brewer;
        }, splitOn: "Split").ToList();
    }
    
    // 3.4 Question
    // 1 op veel relatie (one-to-many relationship)
    // Geef een overzicht van alle bieren. Zorg ervoor dat de property Brewer gevuld is.
    // Sorteer op biernaam en beerId!!!!
    // Zorg ervoor dat bieren van dezelfde brouwerij naar dezelfde instantie van Brouwer verwijzen.
    // Dit kan je doen door een Dictionary<int, Brouwer> te gebruiken.
    // Kijk in voorbeelden hoe je dit kan doen. Deze staan in de directory ExampleFromSheets/Relationships.cs.
    public static List<Beer> GetAllBeersIncludeBrewery()
    {
        var connection = DbHelper.GetConnection();
        Dictionary<int, Brewer> BrewerDictionary = new Dictionary<int, Brewer>();
        string sql = @"select beer.beerid, beer.type, beer.alcohol, beer.name, beer.brewerid, beer.style, 'Split' as Split, brewer.brewerid, brewer.name, brewer.country from beer
                        join brewer on beer.brewerid=brewer.brewerid order by beer.name asc, beer.beerid asc";
        return connection.Query<Beer, Brewer, Beer>(sql, map: (beer, brewer) =>
        {
            beer.Brewer = brewer;
            return beer;
        }, splitOn: "Split" ).ToList();
    }
    
    // 3.5 Question
    // N+1 probleem (1-to-many relationship)
    // Geef een overzicht van alle brouwerijen en hun bieren. Sorteer op brouwerijnaam en daarna op biernaam.
    // Doe dit door eerst een Query<Brewer>(...) te doen die alle brouwerijen ophaalt. (Dit is 1)
    // Loop (foreach) daarna door de brouwerijen en doe voor elke brouwerij een Query<Beer>(...)
    // die de bieren ophaalt voor die brouwerij. (Dit is N)
    // Dit is een N+1 probleem. Hoe los je dit op? Dat zien we in de volgende vragen.
    // Als N groot is (veel brouwerijen) dan kan dit een performance probleem zijn of worden. Probeer dit te voorkomen!
    public static List<Brewer> GetAllBrewersIncludingBeersNPlus1()
    {
        var connection = DbHelper.GetConnection();
        Dictionary<int, Brewer> BrewerDictionary = new Dictionary<int, Brewer>();
        string sql =
            @"select brewer.brewerid, brewer.name, brewer.country, 'Split' as Split, beer.brewerid, beer.name, beer.beerid, beer.style, beer.type, beer.alcohol from brewer
            join beer on brewer.brewerid=beer.brewerid order by brewer.name asc, beer.name asc";
        List<Brewer> brewers = connection.Query<Brewer, Beer, Brewer>(sql, map: (Brewer, beer) =>
        {
            if (BrewerDictionary.ContainsKey(Brewer.BrewerId))
            {
                Brewer = BrewerDictionary[Brewer.BrewerId];
            }
            else
            {
                BrewerDictionary.Add(Brewer.BrewerId, Brewer);
            }
            Brewer.Beers.Add(beer);
            return Brewer;
        }, splitOn: "Split").Distinct().ToList();
        return brewers;
    }
    
    // 3.6 Question
    // 1 op n relatie (one-to-many relationship)
    // Schrijf een query die een overzicht geeft van alle brouwerijen. Vul per brouwerij de property Beers (List<Beer>) met de bieren van die brouwerij.
    // Sorteer op brouwerijnaam en daarna op biernaam.
    // Gebruik de methode Query<Brewer, Beer, Brewer>(sql, map: ...)
    // Het is belangrijk dat je de map functie gebruikt om de bieren te vullen.
    // De query geeft per brouwerij meerdere bieren terug. Dit is een 1 op veel relatie.
    // Om ervoor te zorgen dat de bieren van dezelfde brouwerij naar dezelfde instantie van Brewer verwijzen,
    // moet je een Dictionary<int, Brewer> gebruiken.
    // Dit is een veel voorkomend patroon in Dapper.
    // Vergeet de Distinct() methode te gebruiken om dubbel brouwerijen (Brewer) te voorkomen.
    //  Query<...>(...).Distinct().ToList().
    
    public static List<Brewer> GetAllBrewersIncludeBeers()
    {
        var connection = DbHelper.GetConnection();
        Dictionary<int, Brewer> brewerDictionary = new Dictionary<int, Brewer>();
        string sql =
            @"select brewer.name, brewer.brewerid, brewer.country, beer.type, beer.style, beer.alcohol, beer.brewerid, beer.name, beer.beerid from brewer
                    join beer on brewer.brewerid=beer.brewerid
                    order by brewer.name asc, beer.name asc";
        List<Brewer> brewers = connection.Query<Brewer, Beer, Brewer>(sql, map: (Brewer, Beer) =>
        {
            if (brewerDictionary.ContainsKey(Brewer.BrewerId))
            {
                Brewer = brewerDictionary[Brewer.BrewerId];
            }
            else
            {
                brewerDictionary.Add(Brewer.BrewerId, Brewer);
            }
            Brewer.Beers.Add(Beer);
            return Brewer;

        }, splitOn: "Split").Distinct().ToList();
        return brewers;
    }
    
    // 3.7 Question
    // Optioneel:
    // Dezelfde vraag als hiervoor, echter kan je nu ook de Beers property van Brewer vullen met de bieren?
    // Hiervoor moet je wat extra logica in map methode schrijven.
    // Let op dat er geen dubbelingen komen in de Beers property van Beer!
    public static List<Beer> GetAllBeersIncludeBreweryAndIncludeBeersInBrewery()
    {
        var connection = DbHelper.GetConnection();
        Dictionary<int, Brewer> brewerdictionary = new Dictionary<int, Brewer>();
        string sql = @"select *
                    from beer
                    join brewer on brewer.brewerid = beer.brewerid
                    order by beer.name, beer.Type";
        List<Beer> beers = connection.Query<Beer, Brewer, Beer>(sql, map: (Beer, Brewer) =>
        {
            if (brewerdictionary.ContainsKey(Brewer.BrewerId))
            {
                Brewer = brewerdictionary[Brewer.BrewerId];
            }
            else
            {
                brewerdictionary.Add(Brewer.BrewerId, Brewer);
            }
            Brewer.Beers.Add(Beer);
            Beer.Brewer = Brewer;
            return Beer;
            

        }, splitOn: "BrewerId").Distinct().ToList();
        return beers;
        throw new NotImplementedException();
    }
    
    // 3.8 Question
    // n op n relatie (many-to-many relationship)
    // Geef een overzicht van alle cafés en welke bieren ze schenken.
    // Let op een café kan meerdere bieren schenken. En een bier wordt vaak in meerdere cafe's geschonken. Dit is een n op n relatie.
    // Sommige cafés schenken geen bier. Dus gebruik LEFT JOINS in je query.
    // Bij n op n relaties is er altijd spraken van een tussen-tabel (JOIN-table, associate-table), in dit geval is dat de tabel Sells.
    // Gebruikt de multi-mapper Query<Cafe, Beer, Cafe>("query", splitOn: "splitCol1, splitCol2").
    // Gebruik de klassen Cafe en Beer.
    // De bieren worden opgeslagen in een de property Beers (List<Beer>) van de klasse Cafe.
    // Sorteer op cafénaam en daarna op biernaam.
    
    // Kan je ook uitleggen wat het verschil is tussen de verschillende JOIN's en wat voor gevolg dit heeft voor het resultaat?
    // Het is belangrijk om te weten wat de verschillen zijn tussen de verschillende JOIN's!!!! Als je dit niet weet, zoek het op!
    // Als je dit namelijk verkeerd doet, kan dit grote gevolgen hebben voor je resultaat (je krijgt dan misschien een verkeerde aantal records).
    public static List<Cafe> OverzichtBierenPerKroegLijstMultiMapper()
    {
        var connection = DbHelper.GetConnection();
        Dictionary<int, Cafe> CafeDictionary = new Dictionary<int, Cafe>();
        string sql = @"select beer.beerid, beer.name, 'split1' as split1, cafe.cafeid, cafe.name from cafe
            left join sells on sells.cafeid=cafeid.cafeid left join beers on sells.beerid=cafe.beerid order by cafe.name asc, beer.name asc";
        List<Cafe> cafes = connection.Query<Cafe, Beer, Cafe>(sql, map: (Cafe, Beer) =>
        {
            if (CafeDictionary.ContainsKey(Cafe.CafeId))
            {
                Cafe = CafeDictionary[Cafe.CafeId];
            }
            else
            {
                CafeDictionary.Add(Cafe.CafeId, Cafe);
            }
            Cafe.Beers.Add(Beer);
            return Cafe;
        }, splitOn: "Split").Distinct().ToList();
        return cafes;
    }

    // 3.9 Question
    // We gaan nu nog een niveau dieper. Geef een overzicht van alle brouwerijen, met daarin de bieren die ze verkopen,
    // met daarin in welke cafés ze verkocht worden.
    // Sorteer op brouwerijnaam, biernaam en cafenaam. 
    // Gebruik (vul) de class Brewer, Beer en Cafe.
    // Gebruik de methode Query<Brewer, Beer, Cafe, Brewer>(...) met daarin de juiste JOIN's in de query en splitOn parameter.
    // Je zult twee dictionaries moeten gebruiken. Een voor de brouwerijen en een voor de bieren.
    public static List<Brewer> GetAllBrewersIncludeBeersThenIncludeCafes()
    
    {
        string sql = @"SELECT Brewer.BrewerId,brewer.Name, brewer.Country, 
       '' split1, beer.BeerId, beer.name,beer.type,beer.style,beer.Alcohol,beer.brewerid, 
            '' split2, cafe.CafeId, cafe.name, cafe.Address, cafe.city 
        
FROM Dapperbeer.Brewer 
    left JOIN dapperbeer.beer ON brewer.BrewerId = beer.BrewerId
left JOIN dapperbeer.sells ON beer.BeerId = sells.BeerId
     LEFT       JOIN dapperbeer.cafe ON sells.CafeId = cafe.CafeId
            
            order by brewer.Name, beer.name, cafe.name";

        using IDbConnection connection = DbHelper.GetConnection();

        var brewerDict = new Dictionary<int, Brewer>();
        var beerDict = new Dictionary<int, Beer>();

        List<Brewer> brewers = connection.Query<Brewer, Beer, Cafe, Brewer>(
            sql,
            (brewer, beer, cafe) => {
                if (!brewerDict.TryGetValue(brewer.BrewerId, out var x)) {
                    x = brewer;
                    brewerDict.Add(brewer.BrewerId, x);
                }

                if (!beerDict.TryGetValue(beer.BeerId, out var y)) {
                    y = beer;
                    beerDict.Add(beer.BeerId, y);
                }

                y.Cafes.Add(cafe);
                x.Beers.Add(y);
                return x;
            },
            splitOn: "split1, split2"
        ).Distinct().ToList();

        return brewers;
    }
    
    // 3.10 Question - Er is geen test voor deze vraag
    // Optioneel: Geef een overzicht van alle bieren en hun de bijbehorende brouwerij.
    // Sorteer op brouwerijnaam, biernaam.
    // Gebruik hiervoor een View BeerAndBrewer (maak deze zelf). Deze view bevat alle informatie die je nodig hebt gebruikt join om de tabellen Beer, Brewer.
    // Let op de kolomnamen in de view moeten uniek zijn. Dit moet je dan herstellen in de query waarin je view gebruik zodat Dapper het snap
    // (SELECT BeerId, BeerName as Name, Type, ...). Zie BeerName als voorbeeld hiervan.
    public static List<Beer> GetBeerAndBrewersByView()
    {
        var connection = DbHelper.GetConnection();
        string sql = @"";
        throw new NotImplementedException();
    }
}