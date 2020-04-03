using System;
using System.Buffers;
using System.IO;
using System.Threading.Tasks;

namespace FormatCovid19Data
{
    public sealed class CsvReader : IDisposable
    {
        private static readonly char[] CsvSpecialCharacters = { ',', '"' };

        private readonly TextReader textReader;
        private string? line;
        private int fieldStartPosition;
        private int fieldEndPosition;
        private bool isQuotedField;
        private int escapedQuoteCount;
        private int validUnescapedBufferLength;
        private char[]? unescapedBuffer;

        public CsvReader(TextReader textReader)
        {
            this.textReader = textReader ?? throw new ArgumentNullException(nameof(textReader));
        }

        public void Dispose()
        {
            if (unescapedBuffer is { })
            {
                ArrayPool<char>.Shared.Return(unescapedBuffer);
                unescapedBuffer = null;
            }

            textReader.Dispose();
        }

        public int FieldIndex { get; private set; } = -1;
        public int LineIndex { get; private set; }

        public ReadOnlySpan<char> RawFieldValue =>
            line is null ? default :
            fieldEndPosition == -1 ? line.AsSpan(fieldStartPosition) :
            line.AsSpan(fieldStartPosition, fieldEndPosition - fieldStartPosition);

        public ReadOnlySpan<char> FieldValue
        {
            get
            {
                var span = RawFieldValue;
                if (!isQuotedField) return span;

                span = span.Slice(1, span.Length - 2);
                if (escapedQuoteCount == 0) return span;

                if (validUnescapedBufferLength == 0)
                {
                    validUnescapedBufferLength = span.Length - escapedQuoteCount;

                    if (unescapedBuffer is { } && unescapedBuffer.Length < validUnescapedBufferLength)
                    {
                        ArrayPool<char>.Shared.Return(unescapedBuffer);
                        unescapedBuffer = null;
                    }

                    unescapedBuffer ??= ArrayPool<char>.Shared.Rent(validUnescapedBufferLength);

                    var position = 0;
                    while (true)
                    {
                        var countBeforeQuote = span.IndexOf('"');
                        if (countBeforeQuote == -1) break;

                        var charsToCopy = countBeforeQuote + 1;
                        span.Slice(0, charsToCopy).CopyTo(unescapedBuffer.AsSpan(position));
                        position += charsToCopy;

                        span = span.Slice(countBeforeQuote + 2);
                    }

                    span.CopyTo(unescapedBuffer.AsSpan(position));
                }

                return unescapedBuffer.AsSpan(0, validUnescapedBufferLength);
            }
        }

        public async Task<bool> ReadFieldAsync(int skipFields = 0)
        {
            if (skipFields < 0)
                throw new ArgumentOutOfRangeException(nameof(skipFields), skipFields, "The number of fields to skip must not be negative.");

            for (var i = 0; i <= skipFields; i++)
            {
                if (!await ReadFieldAsync().ConfigureAwait(false))
                    return false;
            }

            return true;
        }

        public async Task<bool> ReadFieldAsync()
        {
            if (fieldEndPosition == -1) return false;

            if (line is null)
            {
                line = await textReader.ReadLineAsync().ConfigureAwait(false);
                if (line is null) return false;
            }
            else if (FieldIndex >= 0)
            {
                fieldStartPosition = fieldEndPosition + 1;
            }

            FieldIndex++;
            escapedQuoteCount = 0;
            validUnescapedBufferLength = 0;

            var specialPosition = line!.IndexOfAny(CsvSpecialCharacters, fieldStartPosition);
            if (specialPosition != -1)
            {
                switch (line[specialPosition])
                {
                    case ',':
                        break;

                    case '"':
                        if (specialPosition != fieldStartPosition)
                            throw new InvalidDataException("Quotes may not appear within unquoted fields.");

                        isQuotedField = true;

                        var quoteSearchPosition = specialPosition + 1;
                        while (true)
                        {
                            var nextQuotePosition = line.IndexOf('"', quoteSearchPosition);
                            if (nextQuotePosition == -1)
                                throw new InvalidDataException("Ending quote not found for quoted field.");

                            if (nextQuotePosition + 1 < line.Length)
                            {
                                switch (line[nextQuotePosition + 1])
                                {
                                    case ',':
                                        fieldEndPosition = nextQuotePosition + 1;
                                        return true;

                                    case '"':
                                        escapedQuoteCount++;
                                        quoteSearchPosition = nextQuotePosition + 2;
                                        continue;

                                    default:
                                        throw new InvalidDataException("Unquoted characters after quoted field.");
                                }
                            }

                            fieldEndPosition = -1;
                            return true;
                        }
                }
            }

            fieldEndPosition = specialPosition;
            isQuotedField = false;
            return true;
        }

        public async Task<bool> NextLineAsync()
        {
            if (line is null)
            {
                if (await textReader.ReadLineAsync().ConfigureAwait(false) is null)
                    return false;
            }

            var nextLine = await textReader.ReadLineAsync().ConfigureAwait(false);
            if (nextLine is null) return false;

            line = nextLine;
            LineIndex++;
            FieldIndex = -1;
            fieldStartPosition = 0;
            fieldEndPosition = 0;
            isQuotedField = false;
            escapedQuoteCount = 0;
            validUnescapedBufferLength = 0;
            return true;
        }
    }
}
