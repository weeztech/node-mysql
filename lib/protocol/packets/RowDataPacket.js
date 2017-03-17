var Types                        = require('../constants/types');
var Charsets                     = require('../constants/charsets');
var Field                        = require('./Field');
var IEEE_754_BINARY_64_PRECISION = Math.pow(2, 53);

module.exports = RowDataPacket;
function RowDataPacket() {
}

Object.defineProperty(RowDataPacket.prototype, 'parse', {
  configurable : true,
  enumerable   : false,
  value        : parse
});

Object.defineProperty(RowDataPacket.prototype, '_typeCast', {
  configurable : true,
  enumerable   : false,
  value        : _typeCast
});

function parse(parser, fieldPackets, typeCast, nestTables, connection,rowsAsArray) {
    var fieldPacket, value,
        config = connection.config,
        supportBigNumbers = config.supportBigNumbers,
        timezone = config.timezone,
        bigNumberStrings = config.bigNumberStrings,
        dateStrings = config.dateStrings;

    function next() {
        return _typeCast(fieldPacket, parser, timezone, supportBigNumbers, bigNumberStrings, dateStrings);
    }

    if (!nestTables && rowsAsArray) {
        this.row = [];
    }

    for (var i = 0; i < fieldPackets.length; i++) {
        fieldPacket = fieldPackets[i];

        if (typeof typeCast === 'function') {
            value = typeCast.apply(connection, [new Field({packet: fieldPacket, parser: parser}), next]);
        } else {
            value = (typeCast)
                ? _typeCast(fieldPacket, parser, timezone, supportBigNumbers, bigNumberStrings, dateStrings)
                : ( (fieldPacket.charsetNr === Charsets.BINARY)
                    ? parser.parseLengthCodedBuffer()
                    : parser.parseLengthCodedString() );
        }

        if (nestTables) {
            if (typeof nestTables === 'string' && nestTables.length) {
                this[fieldPacket.table + nestTables + fieldPacket.name] = value;
            } else {
                this[fieldPacket.table] = this[fieldPacket.table] || {};
                this[fieldPacket.table][fieldPacket.name] = value;
            }
        } else if (rowsAsArray) {
            this.row[i] = value
        } else {
            this[fieldPacket.name] = value;
        }
    }
}
function cantBeNumber(str){
    var last = str.length - 1, c = str.charCodeAt(0), start = c === 45/*-*/ ? 1: 0, hasPoint;
    while(start <= last){
        c = str.charCodeAt(start)
        if (c === 46/*.*/){
            hasPoint = true
        }else if (c !== 48/*0*/){
            break
        }
        start++
    }
    while(start <= last){
        c = str.charCodeAt(last)
        if (c === 46/*.*/){
            hasPoint = true
        }else if (c !== 48/*0*/){
            break
        }
        last--
    }
    var p = last - start
    if (hasPoint){
        return p > 15
    }
    if (p > 16){
        return true
    }else if (p < 15){
        return false
    }
    for(var i= start + 1; i < last; i++){//p in (15,16)
        if (str.charCodeAt(i) === 46/*.*/){
            return p > 15 // p=16
        }
    }
    return true
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

      if (typeMatch(field.type, dateStrings)) {
        return dateString;
      }

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

      var dt = new Date(dateString);
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
      return (numberString === null || (field.zeroFill && numberString[0] === '0'))
        ? numberString : Number(numberString);
    case Types.NEWDECIMAL:
    case Types.LONGLONG:
      numberString = parser.parseLengthCodedString();
        if (numberString === null || (field.zeroFill && numberString[0] === '0')
            || (supportBigNumbers && (bigNumberStrings || cantBeNumber(numberString)))) {
            return numberString;
        } else {
            return Number(numberString);
        }
    case Types.JSON:
      return JSON.parse(parser.parseLengthCodedString());
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
    default:
      return parser.parseLengthCodedString();
  }
}

function typeMatch(type, list) {
  if (Array.isArray(list)) {
    for (var i = 0; i < list.length; i++) {
      if (Types[list[i]] === type) return true;
    }
    return false;
  } else {
    return Boolean(list);
  }
}
