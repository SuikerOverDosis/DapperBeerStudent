using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.InteropServices.JavaScript;
using System.Threading.Tasks;
using DapperBeer.DTO;
using DapperBeer.Model;
using DapperBeer.Tests;

namespace DapperBeer;

using Dapper;

public class Assignments1 : TestHelper
{
    // 1.1 Question
    // Geef een overzicht van alle brouwers, gesorteerd op naam (alfabetisch).
    // Gebruik hiervoor de class Brewer. En de Query<Brewer>(sql) methode van Dapper.
    // Het is ook altijd een goed idee om het resultaat van de Query om te zetten naar een `List<T>`, 
    // Dit kan met de methode ToList() die je aan het einde van de Query() kan toevoegen (connection.Query(sql).ToList()).
    // Onder deze methode staat een test. Zodat je kan controleren of je query en C# code correct werkt.
    // Tip: mocht je een foutmelding krijgen, kijk dan goed naar de foutmelding en de query die je hebt geschreven.
    // Test eerst je SQL-query en kijk dan of je C# code correct werkt met de debugger.
    // Kom je er niet uit na zelf proberen,
    // vraag dan hulp (medestudent, docent). Het kan natuurlijk zijn dat de test niet correct is.
    // Dit is natuurlijk niet de bedoeling (mocht dit zo zijn laat het mij (joris) dan s.v.p. weten). 
    //
    // De test zijn gemaakt met TUnit, dit testframework is relatief nieuw.
    // De tests kan je vinden in de directory Tests.
    // Het kan zijn dat je even een setting moet aanpassen in Rider/Visual Studio. Dit staat beschreven in de README.md.
    // op https://github.com/thomhurst/TUnit
    // LET OP: je kan de testen per stuk runnen, maar ook allemaal tegelijk per Assignment.
    // Maar niet alle assignments (Assigment1.cs, Assignment2.cs, Assignment3.cs) tegelijk!
    // Deze testen zijn niet geschreven om parallel te runnen. Dit zorgt er wel voor dat de testen sneller runnen.
    // 
    // In de directory Model staan de classes die overeenkomen met de database tabellen.
    // In de directory DTO (Data Transfer Object) staan de classes die worden gebruikt als resultaat indien deze
    // niet overeenkomt met een database tabel (Model).
    public static List<Brewer> GetAllBrewers()
    {
        var connection = DbHelper.GetConnection();
        // Het is beter om geen * te gebruiken, maar om kolom namen te gebruiken die overeen komen
        // met de properties van de class Brewer (mijn mening)
        return connection
            .Query<Brewer>("SELECT BrewerId, Name, Country FROM Brewer ORDER BY Name")
            .ToList();
    }
    
    // 1.2 Question
    // Geef een overzicht van alle bieren gesorteerd op alcohol percentage (hoog naar laag).
    public static List<Beer> GetAllBeersOrderByAlcohol()
    {
        string sql = 
            @"  SELECT BeerId, Name, Type, Style, Alcohol, BrewerId 
                FROM Beer
                ORDER BY Alcohol DESC";

        using var connection = DbHelper.GetConnection();
        var beers = connection.Query<Beer>(sql).ToList();
        return beers;
    }
    
    // 1.3 Question
    // Geef een overzicht van ale bieren voor een bepaald land gesorteerd op bier-naam (alfabetisch).
    // Gebruik hiervoor de class Beer. En in je SQL-query JOIN met de tabel Brewer.
    // Gebruik de Query<Beer>(sql, new {Country = country}) methode van Dapper.
    // In je SQL-query kan je de WHERE-clause gebruiken om te filteren op land.
    // Gebruik hiervoor query parameters placeholders in je SQL-query.
    // Dit voorkomt SQL-injectie (onderwerp van les 2).
    //      WHERE brewer.Country = @Country
    // @Country is een query parameter placeholder.
    public static List<Beer> GetAllBeersSortedByNameForCountry(string country)
    {
        var connection = DbHelper.GetConnection();
        string sql = 
            @"select beer.BeerId, beer.Name, beer.Type, beer.Style, beer.Alcohol, beer.BrewerId from Beer join brewer on beer.brewerid=brewer.brewerid where Country = @country order by Name asc";
        return connection.Query<Beer>(sql, new { country }).ToList();
    }
    
    // 1.4 Question
    // Tel het aantal brouwerijen. Welke methode van Dapper gebruik je (niet Query<Brewer>)?
    // Een handige website om te kijken is:
    // https://www.learndapper.com/
    // Voor deze vraag kijken specifiek naar deze pagina: https://www.learndapper.com/dapper-query
    public static int CountBrewers()
    {
        var connection = DbHelper.GetConnection();
        string sql = "SELECT COUNT(*) FROM brewer";
        return connection.QuerySingle<int>(sql);
    }
    
    // 1.5 Question
    // Geef een overzicht van het aantal brouwerijen per land gesorteerd op aantal brouwerijen (van hoog naar laag).
    // Gebruik hiervoor een aparte class NumberOfBrewersByCountry
    // Voeg hiervoor properties toe aan de class NumberOfBrewersByCountry, namelijk Country en NumberOfBreweries.
    // Gebruik de volgende SELECT-clause zodat de kolomnamen in de resultaten overeenkomen met de properties van de class NumberOfBrewersByCountry.:
    //   SELECT Country, COUNT(1) AS NumberOfBreweries
    // In de directory DTO (Data Transfer Object) staan de classes die worden gebruikt als resultaat
    // voor Queries die net overeenkomen met de database tabellen.
    public static List<NumberOfBrewersByCountry> NumberOfBrewersByCountry()
    {
        var connection = DbHelper.GetConnection();
        string sql = "select country, COUNT(1) as NumberOfBreweries from brewer group by Country order by NumberOfBreweries desc";
        return connection.Query<NumberOfBrewersByCountry>(sql).ToList();
    }
    
    // 1.6 Question
    // Geef het bier met het hoogste alcohol percentage terug. Welke methode gebruik je van Dapper (niet Query<Beer>)?
    // Je kan in MySQL de LIMIT 1 gebruiken om 1 record terug te krijgen.
    public static Beer GetBeerWithMostAlcohol()
    {
        var connection = DbHelper.GetConnection();
        string sql = "select * from beer order by alcohol desc limit 1";
        return connection.QuerySingle<Beer>(sql);
    }
    
    // 1.7 Question
    // Gegeven de brewerId geef de brouwer terug. Let op: Wat moet er gebeuren als de brouwcode niet bestaat?
    // Met andere woorden, welke Dapper methode moet je gebruiken? 
    // Brewer? is een nullable type. Dit betekent dat de waarde null kan zijn,
    // indien de brouwerij niet bestaat voor een bepaalde brewerId.
    public static Brewer? GetBreweryByBrewerId(int brewerId)
    {
        var connection = DbHelper.GetConnection();
        string sql = "select * from brewer where BrewerId = @brewerId";
        return connection.QuerySingleOrDefault<Brewer>(sql, new {brewerId});
    }
    
    // 1.8 Question
    // Gegeven de BrewerId, geef een overzicht van alle bieren van de brouwerij gesorteerd bij alcohol percentage.
    public static List<Beer> GetAllBeersByBreweryId(int brewerId)
    {
        using var connection = DbHelper.GetConnection();
        string sql = "select alcohol, style, type, name, beerid, brewerid from beer where brewerid = @brewerId order by alcohol asc";
        return connection.Query<Beer>(sql, new {brewerId}).ToList();
    }
    
    // 1.9 Question
    // Geef per cafe (Cafe) aan welke bier (Beer) ze schenken, sorteer op cafe naam en daarna op bier naam.
    // Gebruik hiervoor de class CafeBeer (directory DTO). 
    public static List<CafeBeer> GetCafeBeers()
    {
        var connection = DbHelper.GetConnection();
        string sql = @"select beer.name as Beers, cafe.name as CafeName from sells
        join cafe on sells.cafeid=cafe.cafeid 
        join beer on sells.beerid=beer.beerid 
        order by CafeName asc, Beers asc";
        return connection.Query<CafeBeer>(sql).ToList();
    }
    
    // De vorige 1.10 Question heb ik verwijderd, deze was nogal lastig
    
    // 1.10 Question
    // Geef de gemiddelde waardering (score in de tabel Review) van een biertje terug gegeven de BeerId.
    public static decimal GetBeerRating(int beerId)
    {
        var connection = DbHelper.GetConnection();
        string sql = "select avg(score) from review where review.Beerid = @BeerId";
        return connection.QuerySingle<decimal>(sql, new {BeerId = beerId});
    }
    
    // 1.11 Question
    // Voeg een review toe voor een bier, met andere woorden gebruik een INSERT.
    // De test werkt alleen als de vorige vraag ook correct is gemaakt.
    public static void InsertReview(int beerId, decimal score)
    {
        var connection = DbHelper.GetConnection();
        string sql = "insert into review (beerid, score) values (@BeerId, @Score)";
        connection.Execute(sql, new {BeerId = beerId, Score = score});
    }
    
    // 1.12 Question
    // Voeg een review toe voor bier. Geef de reviewId terug.
    // Deze test werkt alleen decimal GetBeerRating(int beerId) methode correct is (twee vragen hiervoor).
    public static int InsertReviewReturnsReviewId(int beerId, decimal score)
    {
        var connection = DbHelper.GetConnection();
        string sql = "insert into review (beerid, score) values (@BeerId, @Score); select last_insert_id();";
        return connection.ExecuteScalar<int>(sql, new {BeerId = beerId, Score = score});
    }

    // twee methoden verwijderd
}