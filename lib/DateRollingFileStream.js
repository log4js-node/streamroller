const _ = require('lodash');
const path = require('path');
const debug = require('debug')('streamroller:DateRollingFileStream');
const RollingFileWriteStream = require('./RollingFileWriteStream');

// just to adapt the previous version
class DateRollingFileStream extends RollingFileWriteStream {
  constructor(filename, pattern, options) {
    if (pattern && typeof(pattern) === 'object') {
      options = pattern;
      pattern = null;
    }
    if (!options) {
      options = {};
    }
    if (!pattern) {
      pattern = '.yyyy-MM-dd';
    }
    if (options.daysToKeep) {
      options.daysToKeep += 1;
    }
    options.datePattern = _.trim(pattern, '.').replace(/yy/g, 'YY').replace('dd', 'DD').replace('hh', 'HH');
    options.intervalDays = 1;
    super(filename, options);
    this.filename = this.currentFileStream.path;
    this.pattern = pattern;
    this.mode = this.options.mode;
    this.theStream = this.currentFileStream;
  }

  // just to adapt the previous version
  openTheStream() {}

  // just to adapt the previous version
  closeTheStream(callback) {
    this.end(callback);
  }
}

module.exports = DateRollingFileStream;
