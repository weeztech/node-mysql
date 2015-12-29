var Types = require('../constants/types');
var Charsets = require('../constants/charsets');
var Field = require('./Field');
var IEEE_754_BINARY_64_PRECISION = Math.pow(2, 53);

module.exports = RowArrayDataPacket;
function RowArrayDataPacket() {
}

Object.defineProperty(RowArrayDataPacket.prototype, 'parse', {
    configurable: true,
    enumerable: false,
    value: parse
});

function parse(parser, fieldPackets, typeCast, connection) {
    var self = this, fieldPacket, value,
        supportBigNumbers = connection.config.supportBigNumbers,
        timezone = connection.config.timezone,
        bigNumberStrings = connection.config.bigNumberStrings,
        dateStrings = connection.config.dateStrings;

    function next() {
        return _typeCast(fieldPacket, parser, timezone, supportBigNumbers, bigNumberStrings, dateStrings);
    }

    this.row = [];
    for (var i = 0; i < fieldPackets.length; i++) {
        fieldPacket = fieldPackets[i];

        if (typeof typeCast == "function") {
            value = typeCast.apply(connection, [new Field({packet: fieldPacket, parser: parser}), next]);
        } else {
            value = (typeCast)
                ? _typeCast(fieldPacket, parser, timezone, supportBigNumbers, bigNumberStrings, dateStrings)
                : ( (fieldPacket.charsetNr === Charsets.BINARY)
                ? parser.parseLengthCodedBuffer()
                : parser.parseLengthCodedString() );
        }

        this.row[i] = value
    }
}

function _typeCast(field, parser, timeZone, supportBigNumbers, bigNumberStrings, dateStrings) {
    var numberString;

    switch (field.type) {
        case Types.TIMESTAMP:
        case Types.TIMESTAMP2:
        case Types.DATE:
        case Types.DATETIME:
        case Types.DATETIME2:
        case Types.NEWDATE:
            var dateString = parser.parseLengthCodedString();
            if (dateStrings) {
                return dateString;
            }
            var dt;

            if (dateString === null) {
                return null;
            }

            var originalString = dateString;
            if (field.type === Types.DATE) {
                dateString += ' 00:00:00';
            }

            if (timeZone !== 'local') {
                dateString += ' ' + timeZone;
            }

            dt = new Date(dateString);
            if (isNaN(dt.getTime())) {
                return originalString;
            }

            return dt;
        case Types.TINY:
        case Types.SHORT:
        case Types.LONG:
        case Types.INT24:
        case Types.YEAR:
        case Types.FLOAT:
        case Types.DOUBLE:
            numberString = parser.parseLengthCodedString();
            return (numberString === null || (field.zeroFill && numberString[0] == "0"))
                ? numberString : Number(numberString);
        case Types.NEWDECIMAL:
        case Types.LONGLONG:
            numberString = parser.parseLengthCodedString();
            if ((numberString === null || (field.zeroFill && numberString[0] == "0"))) {
                return numberString;
            } else if (supportBigNumbers) {
                if (bigNumberStrings) {
                    return numberString;
                }
                var nb = Number(numberString);
                return nb > IEEE_754_BINARY_64_PRECISION ? numberString : nb
            } else {
                return Number(numberString);
            }
        case Types.BIT:
            return parser.parseLengthCodedBuffer();
        case Types.STRING:
        case Types.VAR_STRING:
        case Types.TINY_BLOB:
        case Types.MEDIUM_BLOB:
        case Types.LONG_BLOB:
        case Types.BLOB:
            return (field.charsetNr === Charsets.BINARY)
                ? parser.parseLengthCodedBuffer()
                : parser.parseLengthCodedString();
        case Types.GEOMETRY:
            return parser.parseGeometryValue();
        case Types.JSON:
            return JSON.parse(parser.parseLengthCodedString());
        default:
            return parser.parseLengthCodedString();
    }
}
