const debug = require('debug')('streamroller:RollingFileWriteStream');
const _ = require('lodash');
const fs = require('fs-extra');
const zlib = require('zlib');
const path = require('path');
const newNow = require('./now');
const format = require('date-format');
const { Writable } = require('stream');

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
    const defaultOptions = {
      maxSize: Number.MAX_SAFE_INTEGER,
      encoding: 'utf8',
      mode: parseInt('0644', 8),
      flags: 'a',
      compress: false,
      keepFileExt: false,
      alwaysIncludePattern: false
    };
    const options = _.assign({}, defaultOptions, rawOptions);
    if (options.maxSize <= 0) {
      throw new Error(`options.maxSize (${options.maxSize}) should be > 0`);
    }
    if (options.hasOwnProperty('numToKeep') && options.numToKeep < 1) {
      throw new Error(`options.numToKeep (${options.numToKeep}) should be > 0`);
    }
    debug(`creating stream with option=${JSON.stringify(options)}`);
    return options;
  }

  _initState() {
    fs.ensureDirSync(this.fileObject.dir);
    const existingFileDetails = this._getExistingFiles();
    const now = newNow();
    const newFileName = this._formatFileName({isHotFile: true});
    if (existingFileDetails.length > 0
      && _.find(existingFileDetails, f => f.fileName === newFileName)) {
      const hotFileState = fs.statSync(path.format({
        dir: this.fileObject.dir,
        base: newFileName
      }));
      const hotFileDate = new Date(hotFileState.birthtimeMs);
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
      debug(`in _write, should I roll? pattern = ${this.options.pattern}, currentDate = ${this.state.currentDate}`);
      if (this.options.pattern && (format(this.options.pattern, this.state.currentDate) !== format(this.options.pattern, newNow()))) {
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
      n => (n.date ? n.date.valueOf() : newNow().valueOf()) - n.index
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
      debug(`metaStr=${metaStr}, fileName=${fileName}, prefix=${prefix}, suffix=${suffix}`);
    } else {
      const prefix = this.fileObject.base;
      if (!fileName.startsWith(prefix)) {
        return;
      }
      metaStr = fileName.slice(prefix.length);
      debug(`metaStr=${metaStr}, fileName=${fileName}, prefix=${prefix}`);
    }
    if (!metaStr) {
      return {
        index: 0,
        isCompressed
      };
    }
    if (this.options.pattern) {
      const items = _.split(metaStr, FILENAME_SEP);
      const indexStr = items[items.length - 1];
      debug('items: ', items, ', indexStr: ', indexStr);
      if (indexStr !== undefined && indexStr.match(/^\d+$/)) {
        const dateStr = metaStr.slice(0, -1 * (indexStr.length + 1));
        debug(`dateStr is ${dateStr}`);
        return {
            index: parseInt(indexStr, 10),
            date: format.parse(this.options.pattern, dateStr),
            isCompressed
          };
      } else {
        debug(`metaStr is ${metaStr}`);
          return {
            index: 0,
            date: format.parse(this.options.pattern, metaStr),
            isCompressed
          };
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

  _formatFileName({date, index, isHotFile}) {
    debug(`_formatFileName: date=${date}, index=${index}, isHotFile=${isHotFile}`);
    const dateOpt = date || _.get(this, 'state.currentDate') || newNow();
    const dateStr = format(this.options.pattern, dateOpt);
    const indexOpt = index || _.get(this, 'state.currentIndex');
    const oriFileName = this.fileObject.base;
    if (isHotFile) {
      debug(`_formatFileName: includePattern? ${this.options.alwaysIncludePattern}, pattern: ${this.options.pattern}`);
      if (this.options.alwaysIncludePattern && this.options.pattern) {
        debug(`_formatFileName: is hot file, and include pattern, so: ${oriFileName + FILENAME_SEP + dateStr}`);
        return this.options.keepFileExt
          ? this.fileObject.name + FILENAME_SEP + dateStr + this.fileObject.ext
          : oriFileName + FILENAME_SEP + dateStr;
      }
      debug(`_formatFileName: is hot file so, filename: ${oriFileName}`);
      return oriFileName;
    }
    let fileNameExtraItems = [];
    if (this.options.pattern) {
      fileNameExtraItems.push(dateStr);
    }
    if (indexOpt && this.options.maxSize < Number.MAX_SAFE_INTEGER) {
      fileNameExtraItems.push(indexOpt);
    }
    let fileName;
    if (this.options.keepFileExt) {
      const baseFileName = this.fileObject.name + FILENAME_SEP + fileNameExtraItems.join(FILENAME_SEP);
      fileName = baseFileName + this.fileObject.ext;
    } else {
      fileName = oriFileName + FILENAME_SEP + fileNameExtraItems.join(FILENAME_SEP);
    }
    if (this.options.compress) {
      fileName += ZIP_EXT;
    }
    debug(`_formatFileName: ${fileName}`);
    return fileName;
  }

  _roll({isNextPeriod}) {
    debug(`rolling, isNextPeriod ? ${isNextPeriod}`);
    const currentFilePath = this.currentFileStream.path;
    this.currentFileStream.end('', this.options.encoding, e => {
      if (e !== undefined) {
        console.log('Closing file failed.');
        throw e;
      }
    });

    debug(`currentIndex = ${this.state.currentIndex}, numToKeep = ${this.options.numToKeep}`);
    for (let i = _.min([this.state.currentIndex, this.options.numToKeep - 1]); i >= 0; i--) {
      debug(`i = ${i}`);
      const sourceFilePath = i === 0
        ? currentFilePath
        : path.format({
          dir: this.fileObject.dir,
          base: this._formatFileName({date: this.state.currentDate, index: i})
        });
      const targetFilePath = i === 0 && isNextPeriod
        ? path.format({
          dir: this.fileObject.dir,
          base: this._formatFileName({date: this.state.currentDate, index: 1 })
        })
        : path.format({
          dir: this.fileObject.dir,
          base: this._formatFileName({date: this.state.currentDate, index: i + 1})
        });
      this._moveFile(sourceFilePath, targetFilePath, this.options.compress && i === 0);
    }
    if (isNextPeriod) {
      debug(`rolling for next period. state=${JSON.stringify(this.state)}`);
      this.state.currentSize = 0;
      this.state.currentIndex = 0;
      this.state.currentDate = newNow();
    } else {
      debug(`rolling during the same period. state=${JSON.stringify(this.state)}`);
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
    debug(`cleaning dir with existing files passed in = ${givenExistingFileDetails}`);
    let existingFileDetails = givenExistingFileDetails;
    if (!existingFileDetails) {
      debug(`no passed in existing files`);
      existingFileDetails = this._getExistingFiles();
    }
    if (existingFileDetails.length === 0) {
      return false;
    }
    debug(`numToKeep = ${this.options.numToKeep}, existingFiles = ${existingFileDetails.length}`);
    debug('existing files are: ', existingFileDetails);
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
