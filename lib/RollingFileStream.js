const RollingFileWriteStream = require('./RollingFileWriteStream');

// just to adapt the previous version
class RollingFileStream extends RollingFileWriteStream {
  constructor(filename, size, backups, options) {
    if (!options) {
      options = {};
    }
    if (size) {
      options.maxSize = size;
    }
    if (!backups && backups !== 0) {
      backups = 1;
    } else if (backups < 0) {
      throw new Error(`backups (${backups}) should be >= 0`);
    } else if (backups >= Number.MAX_SAFE_INTEGER) {
      // to cater for numToKeep (include the hot file) at Number.MAX_SAFE_INTEGER
      throw new Error(`backups (${backups}) should be < Number.MAX_SAFE_INTEGER`);
    }
    options.numToKeep = backups + 1;
    super(filename, options);
    this.backups = backups;
    this.size = this.options.maxSize;
  }

  get theStream() {
    return this.currentFileStream;
  }

}

module.exports = RollingFileStream;
