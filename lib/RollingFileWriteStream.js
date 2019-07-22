const debug = require("debug")("streamroller:RollingFileWriteStream");
const _ = require("lodash");
const async = require("async");
const fs = require("fs-extra");
const zlib = require("zlib");
const path = require("path");
const newNow = require("./now");
const format = require("date-format");
const { Writable } = require("stream");
const fileNameFormatter = require("./fileNameFormatter");
const fileNameParser = require("./fileNameParser");

const moveAndMaybeCompressFile = (
  sourceFilePath,
  targetFilePath,
  needCompress,
  done
) => {
  if (sourceFilePath === targetFilePath) {
    debug(
      `moveAndMaybeCompressFile: source and target are the same, not doing anything`
    );
    return done();
  }
  fs.access(sourceFilePath, fs.constants.W_OK | fs.constants.R_OK, e => {
    if (e) {
      debug(
        `moveAndMaybeCompressFile: source file path does not exist. not moving. sourceFilePath=${sourceFilePath}`
      );
      return done();
    }

    debug(
      `moveAndMaybeCompressFile: moving file from ${sourceFilePath} to ${targetFilePath} ${
        needCompress ? "with" : "without"
      } compress`
    );
    if (needCompress) {
      fs.createReadStream(sourceFilePath)
        .pipe(zlib.createGzip())
        .pipe(fs.createWriteStream(targetFilePath))
        .on("finish", () => {
          debug(
            `moveAndMaybeCompressFile: finished compressing ${targetFilePath}, deleting ${sourceFilePath}`
          );
          fs.unlink(sourceFilePath, done);
        });
    } else {
      debug(
        `moveAndMaybeCompressFile: deleting file=${targetFilePath}, renaming ${sourceFilePath} to ${targetFilePath}`
      );
      fs.unlink(targetFilePath, () => {
        fs.rename(sourceFilePath, targetFilePath, done);
      });
    }
  });
};

/**
 * RollingFileWriteStream is mainly used when writing to a file rolling by date or size.
 * RollingFileWriteStream inherits from stream.Writable
 */
class RollingFileWriteStream extends Writable {
  /**
   * Create a RollingFileWriteStream
   * @constructor
   * @param {string} filePath - The file path to write.
   * @param {object} options - The extra options
   * @param {number} options.numToKeep - The max numbers of files to keep.
   * @param {number} options.maxSize - The maxSize one file can reach. Unit is Byte.
   *                                   This should be more than 1024. The default is Number.MAX_SAFE_INTEGER.
   * @param {string} options.mode - The mode of the files. The default is '0644'. Refer to stream.writable for more.
   * @param {string} options.flags - The default is 'a'. Refer to stream.flags for more.
   * @param {boolean} options.compress - Whether to compress backup files.
   * @param {boolean} options.keepFileExt - Whether to keep the file extension.
   * @param {string} options.pattern - The date string pattern in the file name.
   * @param {boolean} options.alwaysIncludePattern - Whether to add date to the name of the first file.
   */
  constructor(filePath, options) {
    debug(`creating RollingFileWriteStream. path=${filePath}`);
    super(options);
    this.options = this._parseOption(options);
    this.fileObject = path.parse(filePath);
    if (this.fileObject.dir === "") {
      this.fileObject = path.parse(path.join(process.cwd(), filePath));
    }
    this.fileFormatter = fileNameFormatter({
      file: this.fileObject,
      alwaysIncludeDate: this.options.alwaysIncludePattern,
      needsIndex: this.options.maxSize < Number.MAX_SAFE_INTEGER,
      compress: this.options.compress,
      keepFileExt: this.options.keepFileExt
    });

    this.fileNameParser = fileNameParser({
      file: this.fileObject,
      keepFileExt: this.options.keepFileExt,
      pattern: this.options.pattern
    });

    this.state = {
      currentSize: 0
    };

    if (this.options.pattern) {
      this.state.currentDate = format(this.options.pattern, newNow());
    }

    this.filename = this.fileFormatter({
      index: 0,
      date: this.state.currentDate
    });
    if (this.options.flags === "a") {
      this._setExistingSizeAndDate();
    }

    debug(
      `create new file with no hot file. name=${
        this.filename
      }, state=${JSON.stringify(this.state)}`
    );
    this._renewWriteStream();
  }

  _setExistingSizeAndDate() {
    try {
      const stats = fs.statSync(this.filename);
      this.state.currentSize = stats.size;
      if (this.options.pattern) {
        this.state.currentDate = format(this.options.pattern, stats.birthtime);
      }
    } catch (e) {
      //file does not exist, that's fine - move along
      return;
    }
  }

  _parseOption(rawOptions) {
    const defaultOptions = {
      maxSize: Number.MAX_SAFE_INTEGER,
      numToKeep: Number.MAX_SAFE_INTEGER,
      encoding: "utf8",
      mode: parseInt("0644", 8),
      flags: "a",
      compress: false,
      keepFileExt: false,
      alwaysIncludePattern: false
    };
    const options = _.defaults({}, rawOptions, defaultOptions);
    if (options.maxSize <= 0) {
      throw new Error(`options.maxSize (${options.maxSize}) should be > 0`);
    }
    if (options.numToKeep <= 0) {
      throw new Error(`options.numToKeep (${options.numToKeep}) should be > 0`);
    }
    debug(`creating stream with option=${JSON.stringify(options)}`);
    return options;
  }

  _dateChanged() {
    return (
      this.state.currentDate &&
      this.state.currentDate !== format(this.options.pattern, newNow())
    );
  }

  _tooBig() {
    return this.state.currentSize >= this.options.maxSize;
  }

  _shouldRoll(callback) {
    if (this._dateChanged() || this._tooBig()) {
      debug(
        `rolling because dateChanged? ${this._dateChanged()} or tooBig? ${this._tooBig()}`
      );
      this._roll(callback);
      return;
    }
    callback();
  }

  _write(chunk, encoding, callback) {
    this._shouldRoll(() => {
      debug(
        `writing chunk. ` +
          `file=${this.currentFileStream.path} ` +
          `state=${JSON.stringify(this.state)} ` +
          `chunk=${chunk}`
      );
      this.currentFileStream.write(chunk, encoding, e => {
        this.state.currentSize += chunk.length;
        callback(e);
      });
    });
  }

  // Sorted from the oldest to the latest
  _getExistingFiles(cb) {
    fs.readdir(this.fileObject.dir, (e, files) => {
      debug(`_getExistingFiles: files=${files}`);
      const existingFileDetails = _.chain(files)
        .map(n => this.fileNameParser(n))
        .compact()
        .sortBy(n => (n.timestamp ? n.timestamp : newNow().getTime()) - n.index)
        .value();
      cb(null, existingFileDetails);
    });
  }

  _moveOldFiles(cb) {
    const currentFilePath = this.currentFileStream.path;

    this._getExistingFiles((e, files) => {
      const filesToMove = [];
      const todaysFiles = this.state.currentDate
        ? files.filter(f => f.date === this.state.currentDate)
        : files;
      for (let i = todaysFiles.length; i >= 0; i--) {
        debug(`i = ${i}`);
        const sourceFilePath =
          i === 0
            ? currentFilePath
            : this.fileFormatter({
                date: this.state.currentDate,
                index: i
              });
        const targetFilePath = this.fileFormatter({
          date: this.state.currentDate,
          index: i + 1
        });
        filesToMove.push({ sourceFilePath, targetFilePath });
      }
      debug(`filesToMove = `, filesToMove);
      async.eachOfSeries(
        filesToMove,
        (files, idx, cb1) => {
          debug(
            `src=${files.sourceFilePath}, tgt=${
              files.sourceFilePath
            }, idx=${idx}, pos=${filesToMove.length - 1 - idx}`
          );
          moveAndMaybeCompressFile(
            files.sourceFilePath,
            files.targetFilePath,
            this.options.compress && filesToMove.length - 1 - idx === 0,
            cb1
          );
        },
        () => {
          this.state.currentSize = 0;
          this.state.currentDate = this.state.currentDate
            ? format(this.options.pattern, newNow())
            : null;
          debug(`finished rolling files. state=${JSON.stringify(this.state)}`);
          this._renewWriteStream();
          // wait for the file to be open before cleaning up old ones,
          // otherwise the daysToKeep calculations can be off
          this.currentFileStream.write("", "utf8", () => this._clean(cb));
        }
      );
    });
  }

  _roll(cb) {
    debug(`_roll: closing the current stream`);
    this.currentFileStream.end("", this.options.encoding, () => {
      this._moveOldFiles(cb);
    });
  }

  _renewWriteStream() {
    fs.ensureDirSync(this.fileObject.dir);
    const filePath = this.fileFormatter({
      date: this.state.currentDate,
      index: 0
    });
    const ops = _.pick(this.options, ["flags", "encoding", "mode"]);
    this.currentFileStream = fs.createWriteStream(filePath, ops);
    this.currentFileStream.on("error", e => {
      this.emit("error", e);
    });
  }

  _clean(cb) {
    this._getExistingFiles((e, existingFileDetails) => {
      debug(
        `numToKeep = ${this.options.numToKeep}, existingFiles = ${existingFileDetails.length}`
      );
      debug("existing files are: ", existingFileDetails);
      if (
        this.options.numToKeep > 0 &&
        existingFileDetails.length > this.options.numToKeep
      ) {
        const fileNamesToRemove = _.slice(
          existingFileDetails.map(f => f.filename),
          0,
          existingFileDetails.length - this.options.numToKeep - 1
        );
        this._deleteFiles(fileNamesToRemove, cb);
        return;
      }
      cb();
    });
  }

  _deleteFiles(fileNames, done) {
    debug(`files to delete: ${fileNames}`);
    async.each(
      _.map(fileNames, f => path.format({ dir: this.fileObject.dir, base: f })),
      fs.unlink,
      done
    );
    return;
  }
}

module.exports = RollingFileWriteStream;
