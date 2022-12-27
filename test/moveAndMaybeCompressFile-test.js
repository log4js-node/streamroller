require("should");

const fs = require('fs-extra');
const path = require('path');
const zlib = require('zlib');
const proxyquire = require('proxyquire').noPreserveCache();
const moveAndMaybeCompressFile = require('../lib/moveAndMaybeCompressFile');
const TEST_DIR = path.join(__dirname, `moveAndMaybeCompressFile_${Math.floor(Math.random()*10000)}`);

describe('moveAndMaybeCompressFile', () => {
  beforeEach(async () => {
    await fs.emptyDir(TEST_DIR);
  });

  after(async () => {
    await fs.remove(TEST_DIR);
  });

  it('should move the source file to a new destination', async () => {
    const source = path.join(TEST_DIR, 'test.log');
    const destination = path.join(TEST_DIR, 'moved-test.log');
    await fs.outputFile(source, 'This is the test file.');
    await moveAndMaybeCompressFile(source, destination);

    const contents = await fs.readFile(destination, 'utf8');
    contents.should.equal('This is the test file.');

    const exists = await fs.pathExists(source);
    exists.should.be.false();
  });

  it('should compress the source file at the new destination', async () => {
    const source = path.join(TEST_DIR, 'test.log');
    const destination = path.join(TEST_DIR, 'moved-test.log.gz');
    await fs.outputFile(source, 'This is the test file.');
    const moveAndCompressOptions = {compress: true};
    await moveAndMaybeCompressFile(source, destination, moveAndCompressOptions);

    const zippedContents = await fs.readFile(destination);
    const contents = await new Promise(resolve => {
      zlib.gunzip(zippedContents, (e, data) => {
        resolve(data.toString());
      });
    });
    contents.should.equal('This is the test file.');

    const exists = await fs.pathExists(source);
    exists.should.be.false();
  });

  it('should do nothing if the source file and destination are the same', async () => {
    const source = path.join(TEST_DIR, 'pants.log');
    const destination = path.join(TEST_DIR, 'pants.log');
    await fs.outputFile(source, 'This is the test file.');
    await moveAndMaybeCompressFile(source, destination);

    (await fs.readFile(source, 'utf8')).should.equal('This is the test file.');
  });

  it('should do nothing if the source file does not exist', async () => {
    const source = path.join(TEST_DIR, 'pants.log');
    const destination = path.join(TEST_DIR, 'moved-pants.log');
    await moveAndMaybeCompressFile(source, destination);

    (await fs.pathExists(destination)).should.be.false();
  });

  it('should do nothing if compress is concurrently in progress by another process', async () => {
    const source = path.join(TEST_DIR, 'test.log');
    const destination = path.join(TEST_DIR, 'moved-test.log.gz');
    await fs.outputFile(source, 'This is the test file.');
    // simulate another process has already started writing the destination file
    await fs.outputFile(destination, 'Compressed file.');
    const options = {compress: true};
    await moveAndMaybeCompressFile(source, destination, options);

    (await fs.readFile(source, 'utf8')).should.equal('This is the test file.', 'source file should remain intact');
    (await fs.readFile(destination, 'utf8')).should.equal('Compressed file.', 'destination file should remain');
  });

  it('should remove destination file if readstream error', async () => {
    const moveWithMock = proxyquire('../lib/moveAndMaybeCompressFile', {
      "fs-extra": {
        createReadStream: (...args) => { 
          if (args[0]) {
            // replace test.log with a non-existent file to simulate readstream error
            args[0] = args[0].replace(new RegExp('test.log' + '$'), 'non-exist.log');
          }
          return fs.createReadStream(...args);
        }
      }
    });

    const source = path.join(TEST_DIR, 'test.log');
    const destination = path.join(TEST_DIR, 'moved-test.log.gz');
    await fs.outputFile(source, 'This is the test file.');
    const options = {compress: true};
    await moveWithMock(source, destination, options);

    (await fs.readFile(source, 'utf8')).should.equal('This is the test file.', 'source file should remain intact');
    (await fs.pathExists(destination)).should.be.false('destination file should be removed');
  });

  it('should have destination file if readstream error and remove fails', async () => {
    const moveWithMock = proxyquire('../lib/moveAndMaybeCompressFile', {
      "fs-extra": {
        createReadStream: (...args) => { 
          if (args[0]) {
            // replace test.log with a non-existent file to simulate readstream error
            args[0] = args[0].replace(new RegExp('test.log' + '$'), 'non-exist.log');
          }
          return fs.createReadStream(...args);
        },
        unlink: () => Promise.reject({ code: 'EBUSY', message: 'all gone wrong'}),
      }
    });

    const source = path.join(TEST_DIR, 'test.log');
    const destination = path.join(TEST_DIR, 'moved-test.log.gz');
    await fs.outputFile(source, 'This is the test file.');
    const options = {compress: true};
    await moveWithMock(source, destination, options);

    (await fs.readFile(source, 'utf8')).should.equal('This is the test file.', 'source file should remain intact');
    (await fs.readFile(destination, 'utf8')).should.equal('', 'destination file should remain');
  });

  it('should use copy+truncate if source file is locked (windows)', async () => {
    const moveWithMock = proxyquire('../lib/moveAndMaybeCompressFile', {
      "fs-extra": {
        exists: () => Promise.resolve(true),
        move: () => Promise.reject({ code: 'EBUSY', message: 'all gone wrong'}),
        copy: (fs.copy.bind(fs)),
        truncate: (fs.truncate.bind(fs))
      }
    });

    const source = path.join(TEST_DIR, 'test.log');
    const destination = path.join(TEST_DIR, 'moved-test.log');
    await fs.outputFile(source, 'This is the test file.');
    await moveWithMock(source, destination);

    const contents = await fs.readFile(destination, 'utf8');
    contents.should.equal('This is the test file.');

    // won't delete the source, but it will be empty
    (await fs.readFile(source, 'utf8')).should.be.empty();
  });

  it('should not throw unhandled promise rejection when doing copy+truncate', async () => {
    const moveWithMock = proxyquire('../lib/moveAndMaybeCompressFile', {
      "fs-extra": {
        exists: () => Promise.resolve(true),
        move: () => Promise.reject({ code: 'EBUSY', message: 'all gone wrong'}),
        copy: () => Promise.reject({ code: 'ENOENT', message: 'file deleted halfway'}),
        truncate: (fs.truncate.bind(fs))
      }
    });

    const source = path.join(TEST_DIR, 'test.log');
    const destination = path.join(TEST_DIR, 'moved-test.log');
    await fs.outputFile(source, 'This is the test file.');
    await moveWithMock(source, destination).should.not.be.rejected();
  });

  it('should truncate file if remove fails when compressed (windows)', async () => {
    const moveWithMock = proxyquire('../lib/moveAndMaybeCompressFile', {
      "fs-extra": {
        exists: () => Promise.resolve(true),
        unlink: () => Promise.reject({ code: 'EBUSY', message: 'all gone wrong'}),
        createReadStream: fs.createReadStream.bind(fs),
        truncate: fs.truncate.bind(fs)
      }
    });

    const source = path.join(TEST_DIR, 'test.log');
    const destination = path.join(TEST_DIR, 'moved-test.log.gz');
    await fs.outputFile(source, 'This is the test file.');
    const options = {compress: true};
    await moveWithMock(source, destination, options);

    const zippedContents = await fs.readFile(destination);
    const contents = await new Promise(resolve => {
      zlib.gunzip(zippedContents, (e, data) => {
        resolve(data.toString());
      });
    });
    contents.should.equal('This is the test file.');

    // won't delete the source, but it will be empty
    (await fs.readFile(source, 'utf8')).should.be.empty();
  });

  it('should not affect source file if remove and truncate fails when compressed (windows)', async () => {
    const moveWithMock = proxyquire('../lib/moveAndMaybeCompressFile', {
      "fs-extra": {
        exists: () => Promise.resolve(true),
        unlink: () => Promise.reject({ code: 'EBUSY', message: 'all gone wrong'}),
        createReadStream: fs.createReadStream.bind(fs),
        truncate: () => Promise.reject({ code: 'EBUSY', message: 'all gone wrong'}),
      }
    });

    const source = path.join(TEST_DIR, 'test.log');
    const destination = path.join(TEST_DIR, 'moved-test.log.gz');
    await fs.outputFile(source, 'This is the test file.');
    const options = {compress: true};
    await moveWithMock(source, destination, options);

    const zippedContents = await fs.readFile(destination);
    const contents = await new Promise(resolve => {
      zlib.gunzip(zippedContents, (e, data) => {
        resolve(data.toString());
      });
    });
    contents.should.equal('This is the test file.');

    // won't delete or truncate the source
    (await fs.readFile(source, 'utf8')).should.equal('This is the test file.', 'source file should remain intact');
  });

  it('should compress the source file at the new destination with 0o744 rights', async () => {
    const source = path.join(TEST_DIR, 'test.log');
    const destination = path.join(TEST_DIR, 'moved-test.log.gz');
    await fs.outputFile(source, 'This is the test file.');
    const moveAndCompressOptions = {compress: true, mode:0o744};
    await moveAndMaybeCompressFile(source, destination, moveAndCompressOptions);

    const destinationStats = await fs.stat(destination);
    const destMode = (destinationStats.mode & 0o777).toString(8);
    destMode.should.equalOneOf('744', '666'); // windows does not use unix file modes

    const zippedContents = await fs.readFile(destination);
    const contents = await new Promise(resolve => {
      zlib.gunzip(zippedContents, (e, data) => {
        resolve(data.toString());
      });
    });
    contents.should.equal('This is the test file.');

    const exists = await fs.pathExists(source);
    exists.should.be.false();
  });

  it('should compress the source file at the new destination with 0o400 rights', async () => {
    const source = path.join(TEST_DIR, 'test.log');
    const destination = path.join(TEST_DIR, 'moved-test.log.gz');
    await fs.outputFile(source, 'This is the test file.');
    const moveAndCompressOptions = {compress: true, mode:0o400};
    await moveAndMaybeCompressFile(source, destination, moveAndCompressOptions);

    const destinationStats = await fs.stat(destination);
    const destMode = (destinationStats.mode & 0o777).toString(8);
    destMode.should.equalOneOf('400', '444'); // windows does not use unix file modes

    const zippedContents = await fs.readFile(destination);
    const contents = await new Promise(resolve => {
      zlib.gunzip(zippedContents, (e, data) => {
        resolve(data.toString());
      });
    });
    contents.should.equal('This is the test file.');

    const exists = await fs.pathExists(source);
    exists.should.be.false();
  });
});
