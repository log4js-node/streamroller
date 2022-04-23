const debug = require('debug')('streamroller:moveAndMaybeCompressFile');
const fs = require('fs-extra');
const zlib = require('zlib');

const _parseOption = function(rawOptions){
  const defaultOptions = {
    mode: parseInt("0600", 8),
    compress: false,
  };
  const options = Object.assign({}, defaultOptions, rawOptions);
  debug(`_parseOption: moveAndMaybeCompressFile called with option=${JSON.stringify(options)}`);
  return options;
};

const moveAndMaybeCompressFile = async (
  sourceFilePath,
  targetFilePath,
  options
) => {
  options = _parseOption(options);

  if (sourceFilePath === targetFilePath) {
    debug(`moveAndMaybeCompressFile: source and target are the same, not doing anything`);
    return;
  }

  if (await fs.pathExists(sourceFilePath)) {
    debug(
      `moveAndMaybeCompressFile: moving file from ${sourceFilePath} to ${targetFilePath} ${
        options.compress ? "with" : "without"
      } compress`
    );
    if (options.compress) {
      await new Promise((resolve, reject) => {
        let error = false;
        // to avoid concurrency, the process which can create the file will proceed (using flags wx)
        const writeStream = fs.createWriteStream(targetFilePath, {mode: options.mode, flags: "wx"})
          // wait until writable stream is valid before proceeding to read
          .on("open", () => {
            const readStream = fs.createReadStream(sourceFilePath)
              // wait until readable stream is valid before piping
              .on("open", () => {
                readStream.pipe(zlib.createGzip()).pipe(writeStream);
              })
              .on("error", (e) => {
                debug(`moveAndMaybeCompressFile: error reading ${sourceFilePath}`, e);
                error = e;
                // manually close writable: https://nodejs.org/api/stream.html#readablepipedestination-options
                writeStream.close();
              });
          })
          // wait until writable stream finishes (called even if closed prematurely) before deleting/truncating
          .on("finish", () => {
            if (error) {
              debug(`moveAndMaybeCompressFile: error compressing ${targetFilePath}, deleting ${targetFilePath}`);
              // delete targetFilePath (taking as nothing happened)
              fs.unlink(targetFilePath)
                .then(() => { reject(error); })
                .catch((e) => {
                  debug(`moveAndMaybeCompressFile: error deleting ${targetFilePath}`);
                  reject(e);
                });
            } else {
              debug(`moveAndMaybeCompressFile: finished compressing ${targetFilePath}, deleting ${sourceFilePath}`);
              // delete sourceFilePath
              fs.unlink(sourceFilePath)
                .then(resolve)
                .catch((e) => {
                  debug(`moveAndMaybeCompressFile: error deleting ${sourceFilePath}, truncating instead`, e);
                  // fallback to truncate
                  fs.truncate(sourceFilePath)
                    .then(resolve)
                    .catch((e) => {
                      debug(`moveAndMaybeCompressFile: error truncating ${sourceFilePath}`, e);
                      reject(e);
                    });
                });
            }
          })
          .on("error", (e) => {
            debug(`moveAndMaybeCompressFile: error writing ${targetFilePath}`, e);
            error = e;
            reject(e);
          });
      }).catch(() => {});
    } else {
      debug(
        `moveAndMaybeCompressFile: renaming ${sourceFilePath} to ${targetFilePath}`
      );
      try {
        await fs.move(sourceFilePath, targetFilePath, { overwrite: true });
      } catch (e) {
        debug(`moveAndMaybeCompressFile: error renaming ${sourceFilePath} to ${targetFilePath}`, e);
        debug(`moveAndMaybeCompressFile: trying copy+truncate instead`);
        await fs.copy(sourceFilePath, targetFilePath, { overwrite: true });
        await fs.truncate(sourceFilePath);
      }
    }
  }
};

module.exports = moveAndMaybeCompressFile;
