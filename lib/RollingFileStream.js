'use strict';

const _ = require('lodash');
const debug = require('debug')('streamroller:RollingFileStream');
const RollingFileWriteStream = require('./RollingFileWriteStream');

// just to adapt the previous version
class RollingFileStream extends RollingFileWriteStream {
  constructor(filename, size, backups, options) {
    if (!options) {
      options = {};
    }
    options.intervalDays = 0;
    if (size) {
      options.maxSize = size;
    }
    if (!backups) {
      backups = 1;
    }
    options.numToKeep = backups + 1;
    super(filename, options);
    this.filename = this.currentFileStream.path;
    this.backups = this.options.numToKeep - 1;
    this.size = this.options.maxSize;
    this.theStream = this.currentFileStream;
  }

  // just to adapt the previous version
  openTheStream() {}

  // just to adapt the previous version
  closeTheStream(callback) {
    this.end(callback);
  }
}
module.exports = RollingFileStream;
