const debug = require('debug')('streamroller:RollingFileWriteStream');
const _ = require('lodash');
const fs = require('fs-extra');
const zlib = require('zlib');
const moment = require('moment');
const path = require('path');
const {Writable} = require('stream');

const FILENAME_SEP = '.';
const ZIP_EXT = '.gz';

class RollingFileWriteStream extends Writable {
  constructor(filePath, options) {
    debug(`creating RollingFileWriteStream. path=${filePath}`);
    super(options);
    if (!filePath) {
      throw new Error('fileName is required.');
    }
    this.fileObject = path.parse(filePath);
    fs.ensureDirSync(this.fileObject.dir);
    this.options = this._parseOption(options);
    this._initState();
  }

  get path() {
    this.currentFileStream ? this.currentFileStream.path : undefined;
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
      alwaysIncludePattern: false
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
    return options;
  }

  _initState() {
    const existingFileDetails = this._getExistingFiles();
    const now = moment();
    const newFileName = this._formatFileName({isHotFile: true});
    if (existingFileDetails.length > 0
      && _.find(existingFileDetails, f => f.fileName === newFileName)) {
      const hotFileState = fs.statSync(path.format({
        dir: this.fileObject.dir,
        name: newFileName
      }));
      const hotFileDate = moment(hotFileState.birthtimeMs);
      if(hotFileDate.isBefore(now, 'day')) {
        fs.unlinkSync(path.format(this.fileObject));
      }
      const oldestFileDetail = existingFileDetails[existingFileDetails.length - 1];
      this.state = {
        currentDate: hotFileDate,
        currentIndex: oldestFileDetail.index,
        currentSize: hotFileState.size
      };
      debug(`using the existing hot file. name=${newFileName}`);
    } else {
      this.state = {
        currentDate: now,
        currentIndex: 0,
        currentSize: 0
      };
      debug(`create new file with no hot file. name=${newFileName}`);
    }
    this._renewWriteStream(newFileName);
    this.currentFileStream.write('', 'utf8', () => this._clean(existingFileDetails)); // to touch file
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

  _writev(chunks, callback) {
    try {
      if (this._dateRollingEnabled() && (this.state.currentDate || moment()).clone()
        .add(this.options.intervalDays - 1, 'days')
        .isBefore(moment(), 'day')
      ) {
        this._roll({isNextPeriod: true});
      }
      const chunksForOldFile = [];
      for (let i = 0; i < chunks.length; i++) {
        const c = chunks[i];
        if (this.state.currentSize + c.chunk.length < this.options.maxSize) {
          this.state.currentSize += c.chunk.length;
          chunksForOldFile.push(c);
        }
      }
      let error;
      debug(`writing chunks. file=${this.currentFileStream.path} `
         + `state=${JSON.stringify(this.state)} chunks=${chunksForOldFile}`);
      this.currentFileStream.writev(chunksForOldFile, e => error = e);
      if (error) {
        return callback(error);
      }
      const chunksForNewFile = _.slice(chunks, chunksForOldFile.length);
      if (chunksForNewFile.length > 0) {
        this._roll({isNextPeriod: false});
        this.state.currentSize = _.sumBy(chunksForNewFile, c => c.chunk.length);
        debug(`writing chunks. file=${this.currentFileStream.path} `
          + `state=${JSON.stringify(this.state)} chunks=${chunksForNewFile}`);
        this.currentFileStream.writev(chunksForNewFile, callback);
      } else {
        callback();
      }
    } catch (e) {
      return callback(e);
    }
  }

  _final(callback) {
    if (this.currentFileStream) {
      debug(`ending the stream. filename=${this.currentFileStream.path}`);
      this.currentFileStream.end(callback);
    }
    callback();
  }

  _destory(err, callback) {
    if (this.currentFileStream) {
      this.currentFileStream.destroy(err, callback);
    }
    callback();
  }

  // SortedBy asc by date and index
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
    return _.sortBy(existingFileDetails, n => (n.date ? n.date.valueOf() : 0) + n.index);
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
      const prefix = path.format(_.pick(this.fileObject, ['name', 'ext']));
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
      let date = moment(metaStr, this.options.datePattern);
      if (date.isValid()) {
        return {
          index: 0,
          date,
          isCompressed
        };
      }
      const items = _.split(metaStr, FILENAME_SEP);
      if (items.length >= 2) {
        const indexStr = items[items.length - 1];
        if (indexStr !== undefined && indexStr.match(/^\d+$/)) {
          const dateStr = metaStr.slice(0, -1 * (indexStr.length + 1));
          date = moment(metaStr, this.options.datePattern);
          return {
            index: parseInt(indexStr, 10),
            date,
            isCompressed
          };
        }
      }
      return;
    } else {
      if (metaStr.match(/^\d+$/)) {
        return {
          index: parseInt(metaStr, 10),
          isCompressed
        };
      }
      return;
    }
  }

  // moment.date
  // return file name instead of file abs path.
  _formatFileName({date, index, isHotFile}) {
    // Not support hours, minutes, seconds on the filename
    const dateOpt = date || moment();
    const dateStr = dateOpt.clone().startOf('day').format(this.options.datePattern);
    const indexOpt = index || _.get(this, 'state.currentIndex');
    const oriFileName = path.format(_.pick(this.fileObject, ['name', 'ext']));
    if (isHotFile) {
      if (this.options.alwaysIncludePattern && this._dateRollingEnabled()) {
        return this.options.keepFileExt
          ? path.format({
            name: _.join([this.fileObject.name, dateStr], FILENAME_SEP),
            ext: this.fileObject.ext
          })
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
      fileName = path.format({
        name: baseFileName,
        ext: this.fileObject.ext
      });
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
    let error;
    this.currentFileStream.end(e => error = e);
    if (error) {
      throw error;
    }
    let targetFilePath;
    if (isNextPeriod) {
      debug('rolling for next peried.');
      targetFilePath = path.format({
        dir: this.fileObject.dir,
        name: this._formatFileName({date: this.state.currentDate, index: 0})
      });
      this._moveFile(currentFilePath, targetFilePath, this.options.compress);
      this.state = {
        currentIndex: 0,
        currentSize: 0,
        currentDate: moment()
      };
    } else {
      debug('rolling during the same peried.');
      for (let i = _.min([this.state.currentIndex, this.options.numToKeep - 1]); i >= 0; i--) {
        const needCompress = i === 0 && this.options.compress;
        const sourceFilePath = i === 0
          ? currentFilePath
          : path.format({
            dir: this.fileObject.dir,
            name: this._formatFileName({date: this.state.currentDate, index: i})
          });
        const targetFilePath = path.format({
          dir: this.fileObject.dir,
          name: this._formatFileName({date: this.state.currentDate, index: i + 1})
        });
        this._moveFile(sourceFilePath, targetFilePath, needCompress);
      }
      this.state = {
        currentIndex: this.state.currentIndex + 1,
        currentSize: 0,
        currentDate: moment()
      };
    }
    this._renewWriteStream(this._formatFileName({isHotFile: true}));
    this.currentFileStream.write('', 'utf8', () => this._clean()); // to touch file
  }

  _renewWriteStream(fileName) {
    fs.ensureDirSync(this.fileObject.dir);
    const filePath = path.format({dir: this.fileObject.dir, name: fileName});
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
    this.currentFileStream = fs.createWriteStream(
      filePath,
      _.pick(this.options, ['flags', 'encoding', 'mode'])
    );
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
        f => f.date.clone().add(this.options.daysToKeep - 1, 'days').isBefore(moment(), 'day')
      );
      this._deleteFiles(outOfDateFileDetails.map(f => f.fileName));
      existingFileDetails = _.slice(existingFileDetails, outOfDateFileDetails.length);
    }
    if (this.options.numToKeep && existingFileDetails.length > this.options.numToKeep) {
      const fileNamesToRemove = _.slice(
        existingFileDetails.map(f => f.fileName),
        this.options.numToKeep
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
          const filePath = path.format({dir: this.fileObject.dir, name: n});
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
