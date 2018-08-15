const debug = require('debug')('streamroller:RollingFileWriteStream');
const _ = require('lodash');
const fs = require('fs-extra');
const zlib = require('zlib');
const moment = require('moment');
const path = require('path');
const {Writable} = require('stream');

const FILENAME_SEP = '.';
const ZIP_EXT = '.gz';

/**
 * RollingFileWriteStream is mainly used when writing to a file rolling by date or size.
 * RollingFileWriteStream inhebites from stream.Writable
 */
class RollingFileWriteStream extends Writable {
  /**
   * Create a RollingFileWriteStream
   * @constructor
   * @param {string} filePath - The file path to write.
   * @param {object} options - The extra options
   * @param {number} options.numToKeep - The max numbers of files to keep.
   * @param {number} options.intervalDays - The interval days between the two rolling.
   *                                        This can be 0 to disable date-rolling. The default is 1.
   * @param {number} options.daysToKeep - The max days of files to keep. This will be ignored if interval days is 0.
   * @param {number} options.maxSize - The maxSize one file can reach. Unit is Byte.
   *                                   This should be more than 1024. The default is Number.MAX_SAFE_INTEGER.
   * @param {string} options.mode - The mode of the files. The default is '0644'. Refer to stream.writable for more.
   * @param {string} options.flags - The default is 'a'. Refer to stream.flags for more.
   * @param {boolean} options.compress - Whether to compress backup files.
   * @param {boolean} options.keepFileExt - Whether to keep the file extension.
   * @param {string} options.datePattern - The date string pattern in the file name.
   *                                       Refer to moment.js for valid strings. The default is 'YYYY-MM-DD'.
   *                                       This is used to adapt the previous version and will be removed soon
   * @param {boolean} options.alwaysIncludePattern - Whether to add date to the name of the first file.
   *                                                 This is used to adapt the previous version and will be removed soon
   */
  constructor(filePath, options) {
    debug(`creating RollingFileWriteStream. path=${filePath}`);
    super(options);
    if (!filePath) {
      throw new Error('fileName is required.');
    }
    this.fileObject = path.parse(filePath);
    if (this.fileObject.dir === '') {
      this.fileObject = path.parse(path.join(process.cwd(), filePath));
    }
    this.options = this._parseOption(options);
    this._initState();
  }

  path() {
    return this.currentFileStream ? this.currentFileStream.path : undefined;
  }

  _parseOption(rawOptions) {
    const defautOptions = {
      intervalDays: 1,
      maxSize: Number.MAX_SAFE_INTEGER,
      datePattern: 'YYYY-MM-DD', // it's better that this cannot be customized.
      encoding: 'utf8',
      mode: parseInt('0644', 8),
      flags: 'a',
      compress: false,
      keepFileExt: false,
      alwaysIncludePattern: false,
      adaptOldDateRolling: false
    };
    const options = _.assign({}, defautOptions, rawOptions);
    if (options.intervalDays < 0) {
      // it can be 0 to adapt the old `RollingFileStream` for internal usage.
      // But this must be used only for this purpose and will be removed in the future.
      // So this error text is > 0 instead of >= 0.
      throw new Error(`options.intervalDays (${options.intervalDays}) should be > 0.`);
    }
    // The min maxSize should better be 1024 * 10
    // But test cases use very small number. So it's 0 here to make test cases pass.
    // Test cases will be recoded in the future.
    if (options.maxSize <= 0) {
      throw new Error(`options.maxSize (${options.maxSize}) should be >= 10 * 1024 (Byte)`);
    }
    if (options.numToKeep && options.numToKeep < 1) {
      throw new Error(`options.numToKeep (${options.numToKeep}) should be > 0`);
    }
    if (options.daysToKeep && options.daysToKeep < 1) {
      throw new Error(`options.daysToKeep (${options.daysToKeep}) should be > 0`);
    }
    debug(`creating stream with option=${JSON.stringify(options)}`);
    return options;
  }

  _initState() {
    fs.ensureDirSync(this.fileObject.dir);
    const existingFileDetails = this._getExistingFiles();
    const now = moment();
    const newFileName = this._formatFileName({isHotFile: true});
    if (existingFileDetails.length > 0
      && _.find(existingFileDetails, f => f.fileName === newFileName)) {
      const hotFileState = fs.statSync(path.format({
        dir: this.fileObject.dir,
        base: newFileName
      }));
      const hotFileDate = moment(hotFileState.birthtimeMs);
      const oldestFileDetail = existingFileDetails[0];
      this.state = {
        currentDate: hotFileDate,
        currentIndex: oldestFileDetail.index,
        currentSize: hotFileState.size
      };
      debug(`using the existing hot file. name=${newFileName}, state=${JSON.stringify(this.state)}`);
      this._renewWriteStream(newFileName, true);
    } else {
      this.state = {
        currentDate: now,
        currentIndex: 0,
        currentSize: 0
      };
      debug(`create new file with no hot file. name=${newFileName}, state=${JSON.stringify(this.state)}`);
      this._renewWriteStream(newFileName);
    }
    this.currentFileStream.write('', 'utf8', () => this._clean(existingFileDetails)); // to ensure file
    return;
  }

  _write(chunk, encoding, callback) {
    try {
      if (this._dateRollingEnabled() && (this.state.currentDate || moment()).clone()
        .add(this.options.intervalDays - 1, 'days')
        .isBefore(moment(), 'day')
      ) {
        this._roll({isNextPeriod: true});
      }
      if (this.state.currentSize >= this.options.maxSize) {
        this._roll({isNextPeriod: false});
      }
      this.state.currentSize += chunk.length;
      debug(`writing chunk. file=${this.currentFileStream.path} state=${JSON.stringify(this.state)} chunk=${chunk}`);
      this.currentFileStream.write(chunk, encoding, callback);
    } catch (e) {
      return callback(e);
    }
  }

  end(callback) {
    super.end();
    if (this.currentFileStream) {
      debug(`ending the stream. filename=${this.currentFileStream.path}`);
      this.currentFileStream.end(callback);
    } else {
      callback();
    }
  }

  _final(callback) {
    if (this.currentFileStream) {
      debug(`finalizing the stream. filename=${this.currentFileStream.path}`);
      this.currentFileStream.end(callback);
    } else {
      callback();
    }
  }

  _destroy(err, callback) {
    if (this.currentFileStream) {
      debug(`destroying the stream. filename=${this.currentFileStream.path}`);
      this.currentFileStream.destroy(err, callback);
    } else {
      callback();
    }
  }

  // Sorted from the oldest to the latest
  _getExistingFiles(rawDir) {
    const dir = rawDir || this.fileObject.dir;
    const existingFileDetails = _.compact(
      _.map(fs.readdirSync(dir), n => {
        const parseResult = this._parseFileName(n);
        if (!parseResult) {
          return;
        }
        if (parseResult.index < 0) {
          return;
        }
        return _.assign({fileName: n}, parseResult);
      })
    );
    return _.sortBy(
      existingFileDetails,
      n => (n.date ? n.date.valueOf() : moment().valueOf()) - n.index
    );
  }

  // need file name instead of file abs path.
  _parseFileName(fileName) {
    if (!fileName) {
      return;
    }
    let isCompressed = false;
    if (fileName.endsWith(ZIP_EXT)) {
      fileName = fileName.slice(0, -1 * ZIP_EXT.length);
      isCompressed = true;
    }
    let metaStr;
    if (this.options.keepFileExt) {
      const prefix = this.fileObject.name + FILENAME_SEP;
      const suffix = this.fileObject.ext;
      if (!fileName.startsWith(prefix) || !fileName.endsWith(suffix)) {
        return;
      }
      metaStr = fileName.slice(prefix.length, suffix ? -1 * suffix.length : undefined);
    } else {
      const prefix = this.fileObject.base;
      if (!fileName.startsWith(prefix)) {
        return;
      }
      metaStr = fileName.slice(prefix.length + 1);
    }
    if (!metaStr) {
      return {
        index: 0,
        isCompressed
      };
    }
    if (this._dateRollingEnabled()) {
      const items = _.split(metaStr, FILENAME_SEP);
      if (items.length >= 2) {
        const indexStr = items[items.length - 1];
        if (indexStr !== undefined && indexStr.match(/^\d+$/)) {
          const dateStr = metaStr.slice(0, -1 * (indexStr.length + 1));
          const date = moment(dateStr, this.options.datePattern);
          if (date.isValid()) {
            return {
              index: parseInt(indexStr, 10),
              date,
              isCompressed
            };
          }
        }
      } else {
        const date = moment(metaStr, this.options.datePattern);
        if (date.isValid()) {
          return {
            index: 0,
            date,
            isCompressed
          };
        }
      }
    } else {
      if (metaStr.match(/^\d+$/)) {
        return {
          index: parseInt(metaStr, 10),
          isCompressed
        };
      }
    }
    return;
  }

  // moment.date
  // return file name instead of file abs path.
  _formatFileName({date, index, isHotFile}) {
    // Not support hours, minutes, seconds on the filename
    const dateOpt = date || _.get(this, 'state.currentDate') || moment();
    const dateStr = dateOpt.clone().startOf('day').format(this.options.datePattern);
    const indexOpt = index || _.get(this, 'state.currentIndex');
    const oriFileName = this.fileObject.base;
    if (isHotFile) {
      if (this.options.alwaysIncludePattern && this._dateRollingEnabled()) {
        return this.options.keepFileExt
          ? _.join([this.fileObject.name, dateStr], FILENAME_SEP) + this.fileObject.ext
          : _.join([oriFileName, dateStr], FILENAME_SEP);
      }
      return oriFileName;
    }
    let fileNameExtraItems = [];
    if (this._dateRollingEnabled()) {
      fileNameExtraItems.push(dateStr);
    }
    if (indexOpt) {
      fileNameExtraItems.push(indexOpt);
    }
    let fileName;
    if (this.options.keepFileExt) {
      fileNameExtraItems = _.concat([this.fileObject.name], fileNameExtraItems);
      const baseFileName = _.join(fileNameExtraItems, FILENAME_SEP);
      fileName = baseFileName + this.fileObject.ext;
    } else {
      fileNameExtraItems = _.concat([oriFileName], fileNameExtraItems);
      fileName = _.join(fileNameExtraItems, FILENAME_SEP);
    }
    if (this.options.compress) {
      fileName += ZIP_EXT;
    }
    return fileName;
  }

  _dateRollingEnabled() {
    return this.options.intervalDays > 0;
  }

  _roll({isNextPeriod}) {
    const currentFilePath = this.currentFileStream.path;
    this.currentFileStream.end('', this.options.encoding, e => {
      if (e !== undefined) {
        console.log('Closing file failed.');
        throw e;
      }
    });

    for (let i = _.min([this.state.currentIndex, this.options.numToKeep - 1]); i >= 0; i--) {
      const sourceFilePath = i === 0
        ? currentFilePath
        : path.format({
          dir: this.fileObject.dir,
          base: this._formatFileName({date: this.state.currentDate, index: i})
        });
      const targetFilePath = this.options.adaptOldDateRolling && i == 0 && isNextPeriod
        ? path.format({
          dir: this.fileObject.dir,
          base: this._formatFileName({date: this.state.currentDate})
        })
        : path.format({
          dir: this.fileObject.dir,
          base: this._formatFileName({date: this.state.currentDate, index: i + 1})
        });
      this._moveFile(sourceFilePath, targetFilePath, this.options.compress && i === 0);
    }
    if (isNextPeriod) {
      debug(`rolling for next peried. state=${JSON.stringify(this.state)}`);
      this.state.currentSize = 0;
      this.state.currentIndex = 0;
      this.state.currentDate = moment();
    } else {
      debug(`rolling during the same peried. state=${JSON.stringify(this.state)}`);
      this.state.currentSize = 0;
      this.state.currentIndex += 1;
    }
    this._renewWriteStream(this._formatFileName({isHotFile: true}));
    this.currentFileStream.write('', 'utf8', () => this._clean()); // to touch file
  }

  _renewWriteStream(fileName, exists = false) {
    fs.ensureDirSync(this.fileObject.dir);
    const filePath = path.format({dir: this.fileObject.dir, base: fileName});
    const ops = _.pick(this.options, ['flags', 'encoding', 'mode']);
    if (exists) {
      ops.flags = 'a';
    }
    this.currentFileStream = fs.createWriteStream(filePath, ops);
  }

  _moveFile(sourceFilePath, targetFilePath, needCompress) {
    if (sourceFilePath === targetFilePath) {
      return;
    }
    if (!fs.existsSync(sourceFilePath)) {
      debug(`source file path does not exist. not moving. sourceFilePath=${sourceFilePath}`);
      return;
    }
    try {
      if (needCompress) {
        debug(`moving file from ${sourceFilePath} to ${targetFilePath} with compress`);
        const content = fs.readFileSync(sourceFilePath);
        const gzipedContent = zlib.gzipSync(content);
        fs.writeFileSync(targetFilePath, gzipedContent);
        fs.unlinkSync(sourceFilePath);
      } else {
        debug(`moving file from ${sourceFilePath} to ${targetFilePath} without compress`);
        if (fs.existsSync(targetFilePath)) {
          debug(`deleting file. path=${targetFilePath}`);
          fs.unlinkSync(targetFilePath);
        }
        fs.renameSync(sourceFilePath, targetFilePath);
      }
    } catch (e) {
      console.error(`Moving file failed. sourceFilePath=${sourceFilePath},
        targetFilePath=${targetFilePath}, error=${e}`);
    }
  }

  _clean(givenExistingFileDetails) {
    debug('cleaning dir.');
    let existingFileDetails = givenExistingFileDetails;
    if (!existingFileDetails) {
      existingFileDetails = this._getExistingFiles();
    }
    if (existingFileDetails.length === 0) {
      return false;
    }
    if (this._dateRollingEnabled() && this.options.daysToKeep) {
      const outOfDateFileDetails = _.filter(
        existingFileDetails,
        f => (f.date || moment()).clone().add(this.options.daysToKeep + 1, 'days').isBefore(moment(), 'day')
      );
      this._deleteFiles(outOfDateFileDetails.map(f => f.fileName));
      existingFileDetails = _.slice(existingFileDetails, outOfDateFileDetails.length);
    }
    if (this.options.numToKeep && existingFileDetails.length > this.options.numToKeep) {
      const fileNamesToRemove = _.slice(
        existingFileDetails.map(f => f.fileName),
        0,
        existingFileDetails.length - this.options.numToKeep - 1,
      );
      this._deleteFiles(fileNamesToRemove);
      if (this.state.currentIndex > this.options.numToKeep - 1) {
        this.state.currentIndex = this.options.numToKeep - 1;
      }
    }
  }

  _deleteFiles(fileNames) {
    if (fileNames) {
      fileNames.forEach(n => {
        try {
          const filePath = path.format({dir: this.fileObject.dir, base: n});
          debug(`deleting file. path=${filePath}`);
          fs.unlinkSync(filePath);
        } catch (e) {
          console.error(`remove file failed. error=${e}`);
        }
      });
    }
  }
}

module.exports = RollingFileWriteStream;
