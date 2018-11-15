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
      pattern = 'yyyy-MM-dd';
    }
    if (options.daysToKeep) {
      options.numToKeep = options.daysToKeep;
    }
    if (pattern.startsWith('.')) {
      pattern = pattern.substring(1);
    }
    options.pattern = pattern;
    super(filename, options);
    this.filename = this.currentFileStream.path;
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
