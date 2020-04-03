using NUnit.Framework;
using Shouldly;
using System.IO;
using System.Threading.Tasks;

namespace FormatCovid19Data.Tests
{
    public static class CsvReaderTests
    {
        [Test]
        public static void Value_is_initially_empty()
        {
            using var reader = new CsvReader(new StringReader("a,b,c"));

            reader.FieldValue.IsEmpty.ShouldBeTrue();
        }

        [Test]
        public static void FieldIndex_is_initially_negative_one()
        {
            using var reader = new CsvReader(new StringReader("a,b,c"));

            reader.FieldIndex.ShouldBe(-1);
        }

        [Test]
        public static void LineIndex_is_initially_zero()
        {
            using var reader = new CsvReader(new StringReader("a,b,c"));

            reader.LineIndex.ShouldBe(0);
        }

        [Test]
        public static async Task ReadFieldAsync_can_be_called_right_away()
        {
            using var reader = new CsvReader(new StringReader("a,b,c"));

            (await reader.ReadFieldAsync()).ShouldBeTrue();

            reader.FieldValue.ToString().ShouldBe("a");
        }

        [Test]
        public static async Task Subsequent_call_to_ReadFieldAsync_advances_to_the_next_field()
        {
            using var reader = new CsvReader(new StringReader("a,b,c"));

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(0);

            (await reader.ReadFieldAsync()).ShouldBeTrue();

            reader.FieldIndex.ShouldBe(1);
            reader.FieldValue.ToString().ShouldBe("b");
        }

        [Test]
        public static async Task ReadFieldAsync_advances_to_the_last_field_on_the_line()
        {
            using var reader = new CsvReader(new StringReader("a,b\r\nc,d"));

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(0);

            (await reader.ReadFieldAsync()).ShouldBeTrue();

            reader.FieldIndex.ShouldBe(1);
            reader.FieldValue.ToString().ShouldBe("b");
        }

        [Test]
        public static async Task ReadFieldAsync_returns_false_after_last_field_on_the_line()
        {
            using var reader = new CsvReader(new StringReader("a,b\r\nc,d"));

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(1);

            (await reader.ReadFieldAsync()).ShouldBeFalse();
            reader.FieldIndex.ShouldBe(1);

            (await reader.ReadFieldAsync()).ShouldBeFalse();
            reader.FieldIndex.ShouldBe(1);
        }

        [Test]
        public static async Task ReadFieldAsync_advances_to_the_last_field_on_the_last_line()
        {
            using var reader = new CsvReader(new StringReader("a,b"));

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(0);

            (await reader.ReadFieldAsync()).ShouldBeTrue();

            reader.FieldIndex.ShouldBe(1);
            reader.FieldValue.ToString().ShouldBe("b");
        }

        [Test]
        public static async Task ReadFieldAsync_returns_false_after_last_field_on_the_last_line()
        {
            using var reader = new CsvReader(new StringReader("a,b"));

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(1);

            (await reader.ReadFieldAsync()).ShouldBeFalse();
            reader.FieldIndex.ShouldBe(1);

            (await reader.ReadFieldAsync()).ShouldBeFalse();
            reader.FieldIndex.ShouldBe(1);
        }

        [Test]
        public static async Task ReadFieldAsync_stops_on_empty_fields()
        {
            using var reader = new CsvReader(new StringReader(",,a,,,b,,"));

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(0);
            reader.FieldValue.IsEmpty.ShouldBeTrue();

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(1);
            reader.FieldValue.IsEmpty.ShouldBeTrue();

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(2);
            reader.FieldValue.ToString().ShouldBe("a");

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(3);
            reader.FieldValue.IsEmpty.ShouldBeTrue();

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(4);
            reader.FieldValue.IsEmpty.ShouldBeTrue();

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(5);
            reader.FieldValue.ToString().ShouldBe("b");

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(6);
            reader.FieldValue.IsEmpty.ShouldBeTrue();

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(7);
            reader.FieldValue.IsEmpty.ShouldBeTrue();

            (await reader.ReadFieldAsync()).ShouldBeFalse();
        }

        [Test]
        public static async Task Whitespace_is_not_trimmed()
        {
            using var reader = new CsvReader(new StringReader(" a ,\t, "));

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(0);
            reader.FieldValue.ToString().ShouldBe(" a ");

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(1);
            reader.FieldValue.ToString().ShouldBe("\t");

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(2);
            reader.FieldValue.ToString().ShouldBe(" ");
        }

        [Test]
        public static async Task ReadFieldAsync_does_not_split_on_comma_inside_quotes()
        {
            using var reader = new CsvReader(new StringReader("\"a,b\",\",,,\",\",c\""));

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(0);
            reader.FieldValue.ToString().ShouldBe("a,b");

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(1);
            reader.FieldValue.ToString().ShouldBe(",,,");

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(2);
            reader.FieldValue.ToString().ShouldBe(",c");
        }

        [Test]
        public static async Task Quotes_can_be_escaped()
        {
            using var reader = new CsvReader(new StringReader("\"\"\"\",\"a\"\"b\""));

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(0);
            reader.FieldValue.ToString().ShouldBe("\"");

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldIndex.ShouldBe(1);
            reader.FieldValue.ToString().ShouldBe("a\"b");
        }

        [Test]
        public static async Task InvalidDataException_thrown_for_quote_inside_unquoted_value()
        {
            using var reader = new CsvReader(new StringReader("a\"b"));

            var ex = await Should.ThrowAsync<InvalidDataException>(() => reader.ReadFieldAsync());
            ex.Message.ShouldBe("Quotes may not appear within unquoted fields.");
        }

        [Test]
        public static async Task InvalidDataException_thrown_for_missing_end_quote()
        {
            using var reader = new CsvReader(new StringReader("\""));

            var ex = await Should.ThrowAsync<InvalidDataException>(() => reader.ReadFieldAsync());
            ex.Message.ShouldBe("Ending quote not found for quoted field.");
        }

        [Test]
        public static async Task InvalidDataException_thrown_for_missing_end_quote_with_escaped_quotes()
        {
            using var reader = new CsvReader(new StringReader("\"a\"\""));

            var ex = await Should.ThrowAsync<InvalidDataException>(() => reader.ReadFieldAsync());
            ex.Message.ShouldBe("Ending quote not found for quoted field.");
        }

        [Test]
        public static async Task InvalidDataException_thrown_for_unquoted_characters_after_quoted_field()
        {
            using var reader = new CsvReader(new StringReader("\"a\"b,c"));

            var ex = await Should.ThrowAsync<InvalidDataException>(() => reader.ReadFieldAsync());
            ex.Message.ShouldBe("Unquoted characters after quoted field.");
        }

        [Test]
        public static async Task NextLineAsync_can_be_used_immediately()
        {
            using var reader = new CsvReader(new StringReader("a,b\r\nc,d"));

            reader.LineIndex.ShouldBe(0);
            (await reader.NextLineAsync()).ShouldBeTrue();
            reader.LineIndex.ShouldBe(1);

            (await reader.ReadFieldAsync()).ShouldBeTrue();
            reader.FieldValue.ToString().ShouldBe("c");
        }
    }
}
