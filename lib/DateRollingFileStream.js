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
    options.pattern = pattern;
    if (!options.daysToKeep && options.daysToKeep !== 0) {
      options.daysToKeep = 1;
    } else if (options.daysToKeep < 0) {
      throw new Error(`options.daysToKeep (${options.daysToKeep}) should be >= 0`);
    } else if (options.daysToKeep >= Number.MAX_SAFE_INTEGER) {
      // to cater for numToKeep (include the hot file) at Number.MAX_SAFE_INTEGER
      throw new Error(`options.daysToKeep (${options.daysToKeep}) should be < Number.MAX_SAFE_INTEGER`);
    }
    options.numToKeep = options.daysToKeep + 1;
    super(filename, options);
    this.mode = this.options.mode;
  }

  get theStream() {
    return this.currentFileStream;
  }

}

module.exports = DateRollingFileStream;
