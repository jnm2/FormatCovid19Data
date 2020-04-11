using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace FormatCovid19Data
{
    public static class Program
    {
        public static async Task Main()
        {
            using var client = new HttpClient();

            var worldAndCountry = await GetWorldAndCountryCountsAsync(client, "US");
            var stateAndCounty = await GetStateAndCountyDataAsync(client, "Pennsylvania", "Lancaster");

            foreach (var (a, b) in worldAndCountry.Zip(stateAndCounty))
            {
                if (a.Date != b.Date) throw new NotImplementedException();

                Console.WriteLine($"{a.Date:d}\t{a.World}\t{a.Country}\t{b.State}\t{b.County}");
            }
        }

        private static async Task<ImmutableArray<(DateTime Date, int World, int Country)>> GetWorldAndCountryCountsAsync(HttpClient client, string countryName)
        {
            await using var stream = await client.GetStreamAsync("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv");
            using var reader = new CsvReader(new StreamReader(stream));

            await reader.ReadFieldAsync(skipFields: 1);
            RuntimeAssert(reader.FieldValue.Equals("Country/Region", StringComparison.OrdinalIgnoreCase), "The second column should contain country names.");
            await reader.ReadFieldAsync(skipFields: 1);
            RuntimeAssert(!DateTime.TryParse(reader.FieldValue, out _), "Date columns do not start before the 4th column.");

            var columnHeaders = new List<DateTime>();

            while (await reader.ReadFieldAsync())
            {
                columnHeaders.Add(DateTime.ParseExact(reader.FieldValue, "M/d/yy", CultureInfo.InvariantCulture));
            }

            var countsByDate = columnHeaders.Select(date => (Date: date, World: 0, Country: 0)).ToArray();

            while (await reader.NextLineAsync())
            {
                await reader.ReadFieldAsync(skipFields: 1);

                var isCountry = reader.FieldValue.Equals(countryName, StringComparison.OrdinalIgnoreCase);

                await reader.ReadFieldAsync(skipFields: 1);

                for (var index = 0; await reader.ReadFieldAsync(); index++)
                {
                    var count = int.Parse(reader.FieldValue, NumberStyles.None, CultureInfo.InvariantCulture);

                    countsByDate[index].World += count;
                    if (isCountry) countsByDate[index].Country = count;
                }
            }

            return countsByDate.ToImmutableArray();
        }

        private static async Task<ImmutableArray<(DateTime Date, int State, int County)>> GetStateAndCountyDataAsync(HttpClient client, string stateName, string countyName)
        {
            await using var stream = await client.GetStreamAsync("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv");
            using var reader = new CsvReader(new StreamReader(stream));

            await reader.ReadFieldAsync(skipFields: 5);
            RuntimeAssert(reader.FieldValue.Equals("Admin2", StringComparison.OrdinalIgnoreCase), "The 6th column should contain county names.");
            await reader.ReadFieldAsync();
            RuntimeAssert(reader.FieldValue.Equals("Province_State", StringComparison.OrdinalIgnoreCase), "The 7th column should contain state names.");
            await reader.ReadFieldAsync(skipFields: 3);
            RuntimeAssert(!DateTime.TryParse(reader.FieldValue, out _), "Date columns do not start before the 4th column.");

            var columnHeaders = new List<DateTime>();

            while (await reader.ReadFieldAsync())
            {
                columnHeaders.Add(DateTime.ParseExact(reader.FieldValue, "M/d/yy", CultureInfo.InvariantCulture));
            }

            var countsByDate = columnHeaders.Select(date => (Date: date, State: 0, County: 0)).ToArray();

            while (await reader.NextLineAsync())
            {
                await reader.ReadFieldAsync(skipFields: 5);
                var isCounty = reader.FieldValue.Equals(countyName, StringComparison.OrdinalIgnoreCase);

                await reader.ReadFieldAsync();
                if (!reader.FieldValue.Equals(stateName, StringComparison.OrdinalIgnoreCase)) continue;

                await reader.ReadFieldAsync(skipFields: 3);

                for (var index = 0; await reader.ReadFieldAsync(); index++)
                {
                    var count = int.Parse(reader.FieldValue, NumberStyles.None, CultureInfo.InvariantCulture);

                    countsByDate[index].State += count;
                    if (isCounty) countsByDate[index].County = count;
                }
            }

            return countsByDate.ToImmutableArray();
        }

        [DebuggerNonUserCode]
        private static void RuntimeAssert(bool condition, string message)
        {
            if (!condition) throw new InvalidOperationException(message);
        }
    }
}
