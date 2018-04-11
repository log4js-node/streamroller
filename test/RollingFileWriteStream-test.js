const _ = require('lodash');
const path = require('path');
const zlib = require('zlib');
const async = require('async');
const stream = require('stream');
const fs = require('fs-extra');
const proxyquire = require('proxyquire').noPreserveCache();

let fakeNow = new Date(2012, 8, 12, 10, 37, 11);
const mockMoment = require('moment');
mockMoment.now = () => fakeNow;
const RollingFileWriteStream = proxyquire('../lib/RollingFileWriteStream', {
  moment: mockMoment
});

function generateTestFile(fileName) {
  const dirName = path.join(__dirname, 'tmp_' + Math.floor(Math.random() * new Date()));
  fileName = fileName || (Math.floor(Math.random() * Math.floor(2)) ? 'withExtension.log' : 'noExtension');
  const fileNameObj = path.parse(fileName);
  return {
    dir: dirName,
    base: fileNameObj.base,
    name: fileNameObj.name,
    ext: fileNameObj.ext,
    path: path.join(dirName, fileName)
  };
}

describe('RollingFileWriteStream', () => {

  describe('with default arguments', () => {
    const fileObj = generateTestFile();
    let s;

    before(done => {
      s = new RollingFileWriteStream(fileObj.path);
      done();
    });

    after(done => {
      s.end();
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should take a filename and options, return Writable', () => {
      s.should.be.an.instanceOf(stream.Writable);
      s.path().should.eql(fileObj.path);
      s.currentFileStream.mode.should.eql(420);
      s.currentFileStream.flags.should.eql('a');
    });

    it('should apply default options', () => {
      s.options.intervalDays.should.eql(1);
      s.options.maxSize.should.eql(Number.MAX_SAFE_INTEGER);
      s.options.datePattern.should.eql('YYYY-MM-DD');
      s.options.encoding.should.eql('utf8');
      s.options.mode.should.eql(420);
      s.options.flags.should.eql('a');
      s.options.compress.should.eql(false);
      s.options.keepFileExt.should.eql(false);
    });
  });

  describe('with 5 maxSize and 3 interval days', () => {
    const fileObj = generateTestFile();
    let s;

    before(done => {
      fakeNow = new Date(2012, 8, 12, 10, 37, 11);
      s = new RollingFileWriteStream(fileObj.path, {intervalDays: 3, maxSize: 5});
      const flows = Array.from(Array(38).keys()).map(i => cb => {
        fakeNow = new Date(2012, 8, 12 + parseInt(i / 5, 10), 10, 37, 11);
        s.write(i.toString(), 'utf8', cb);
      });
      async.waterfall(flows, () => done());
    });

    after(done => {
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should rotate after 3 days', () => {
      const files = fs.readdirSync(fileObj.dir);
      const expectedFileList = [
        fileObj.base,
        fileObj.base + '.2012-09-12.1',
        fileObj.base + '.2012-09-12.2',
        fileObj.base + '.2012-09-12.3',
        fileObj.base + '.2012-09-12.4',
        fileObj.base + '.2012-09-15.1',
        fileObj.base + '.2012-09-15.2',
        fileObj.base + '.2012-09-15.3',
        fileObj.base + '.2012-09-15.4',
        fileObj.base + '.2012-09-15.5',
        fileObj.base + '.2012-09-18.1',
        fileObj.base + '.2012-09-18.2'
      ];
      files.length.should.equal(expectedFileList.length);
      files.should.containDeep(expectedFileList);
      fs.readFileSync(path.format(fileObj)).toString().should.equal('3637');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-12.1',
      }))).toString().should.equal('1314');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-12.2',
      }))).toString().should.equal('101112');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-12.3',
      }))).toString().should.equal('56789');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-12.4',
      }))).toString().should.equal('01234');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-15.1',
      }))).toString().should.equal('272829');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-15.2',
      }))).toString().should.equal('242526');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-15.3',
      }))).toString().should.equal('212223');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-15.4',
      }))).toString().should.equal('181920');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-15.5',
      }))).toString().should.equal('151617');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-18.1',
      }))).toString().should.equal('333435');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-18.2',
      }))).toString().should.equal('303132');
    });
  });

  describe('with 5 maxSize, 3 interval days and 3 files limit', () => {
    const fileObj = generateTestFile();
    let s;

    before(done => {
      fakeNow = new Date(2012, 8, 12, 10, 37, 11);
      s = new RollingFileWriteStream(fileObj.path, {
        intervalDays: 3,
        maxSize: 5,
        numToKeep: 3
      });
      const flows = Array.from(Array(38).keys()).map(i => cb => {
        fakeNow = new Date(2012, 8, 12 + parseInt(i / 5), 10, 37, 11);
        s.write(i.toString(), 'utf8', cb);
      });
      async.waterfall(flows, () => done());
    });

    after(done => {
      s.end();
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should rotate after 3 days with at most 3 backup files not including the hot one', () => {
      const files = fs.readdirSync(fileObj.dir);
      const expectedFileList = [
        fileObj.base,
        fileObj.base + '.2012-09-15.1',
        fileObj.base + '.2012-09-18.1',
        fileObj.base + '.2012-09-18.2'
      ];
      files.length.should.equal(expectedFileList.length);
      files.should.containDeep(expectedFileList);
      fs.readFileSync(path.format(fileObj)).toString().should.equal('3637');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-15.1'
      }))).toString().should.equal('272829');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-18.1'
      }))).toString().should.equal('333435');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-18.2'
      }))).toString().should.equal('303132');
    });
  });

  describe('with 5 maxSize, 3 interval days and 4 days limit', () => {
    const fileObj = generateTestFile();
    let s;

    before(done => {
      fakeNow = new Date(2012, 8, 12, 10, 37, 11);
      s = new RollingFileWriteStream(fileObj.path, {
        intervalDays: 3,
        maxSize: 5,
        daysToKeep: 3
      });
      const flows = Array.from(Array(38).keys()).map(i => cb => {
        fakeNow = new Date(2012, 8, 12 + parseInt(i / 5), 10, 37, 11);
        s.write(i.toString(), 'utf8', cb);
      });
      async.waterfall(flows, () => done());
    });

    after(done => {
      s.end();
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should rotate after 3 days with at most 3 backup files not including the hot one', () => {
      const files = fs.readdirSync(fileObj.dir);
      const expectedFileList = [
        fileObj.base,
        fileObj.base + '.2012-09-15.1',
        fileObj.base + '.2012-09-15.2',
        fileObj.base + '.2012-09-15.3',
        fileObj.base + '.2012-09-15.4',
        fileObj.base + '.2012-09-15.5',
        fileObj.base + '.2012-09-18.1',
        fileObj.base + '.2012-09-18.2'
      ];
      files.length.should.equal(expectedFileList.length);
      files.should.containDeep(expectedFileList);
      fs.readFileSync(path.format(fileObj)).toString().should.equal('3637');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-15.1',
      }))).toString().should.equal('272829');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-15.2',
      }))).toString().should.equal('242526');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-15.3',
      }))).toString().should.equal('212223');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-15.4',
      }))).toString().should.equal('181920');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-15.5',
      }))).toString().should.equal('151617');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-18.1',
      }))).toString().should.equal('333435');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-18.2',
      }))).toString().should.equal('303132');
    });
  });

  describe('with date pattern DD-MM-YYYY', () => {
    const fileObj = generateTestFile();
    let s;

    before(done => {
      fakeNow = new Date(2012, 8, 12, 10, 37, 11);
      s = new RollingFileWriteStream(fileObj.path, {
        intervalDays: 3,
        maxSize: 5,
        datePattern: 'DD-MM-YYYY'
      });
      const flows = Array.from(Array(8).keys()).map(i => cb => {
        fakeNow = new Date(2012, 8, 12 + parseInt(i / 5, 10), 10, 37, 11);
        s.write(i.toString(), 'utf8', cb);
      });
      async.waterfall(flows, () => done());
    });

    after(done => {
      s.end();
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should rotate with date pattern DD-MM-YYYY in the file name', () => {
      const files = fs.readdirSync(fileObj.dir);
      const expectedFileList = [
        fileObj.base,
        fileObj.base + '.12-09-2012.1'
      ];
      files.length.should.equal(expectedFileList.length);
      files.should.containDeep(expectedFileList);
      fs.readFileSync(path.format(fileObj)).toString().should.equal('567');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.12-09-2012.1'
      }))).toString().should.equal('01234');
    });
  });

  describe('with compress true', () => {
    const fileObj = generateTestFile();
    let s;

    before(done => {
      fakeNow = new Date(2012, 8, 12, 10, 37, 11);
      s = new RollingFileWriteStream(fileObj.path, {
        intervalDays: 3,
        maxSize: 5,
        compress: true
      });
      const flows = Array.from(Array(8).keys()).map(i => cb => {
        fakeNow = new Date(2012, 8, 12 + parseInt(i / 5, 10), 10, 37, 11);
        s.write(i.toString(), 'utf8', cb);
      });
      async.waterfall(flows, () => done());
    });

    after(done => {
      s.end();
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should rotate with gunzip', () => {
      const files = fs.readdirSync(fileObj.dir);
      const expectedFileList = [
        fileObj.base,
        fileObj.base + '.2012-09-12.1.gz'
      ];
      files.length.should.equal(expectedFileList.length);
      files.should.containDeep(expectedFileList);

      fs.readFileSync(path.format(fileObj)).toString().should.equal('567');
      const content = fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.base + '.2012-09-12.1.gz'
      })));
      zlib.gunzipSync(content).toString().should.equal('01234');
    });
  });

  describe('with keepFileExt', () => {
    const fileObj = generateTestFile('keepFileExt.log');
    let s;

    before(done => {
      fakeNow = new Date(2012, 8, 12, 10, 37, 11);
      s = new RollingFileWriteStream(fileObj.path, {
        intervalDays: 3,
        maxSize: 5,
        keepFileExt: true
      });
      const flows = Array.from(Array(8).keys()).map(i => cb => {
        fakeNow = new Date(2012, 8, 12 + parseInt(i / 5, 10), 10, 37, 11);
        s.write(i.toString(), 'utf8', cb);
      });
      async.waterfall(flows, () => done());
    });

    after(done => {
      s.end();
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should rotate with the same extension', () => {
      const files = fs.readdirSync(fileObj.dir);
      const expectedFileList = [
        fileObj.base,
        fileObj.name + '.2012-09-12.1.log'
      ];
      files.length.should.equal(expectedFileList.length);
      files.should.containDeep(expectedFileList);

      fs.readFileSync(path.format(fileObj)).toString().should.equal('567');
      fs.readFileSync(path.format({
        dir: fileObj.dir,
        base: fileObj.name + '.2012-09-12.1' + fileObj.ext
      })).toString().should.equal('01234');
    });
  });

  describe('with keepFileExt and compress', () => {
    const fileObj = generateTestFile('keepFileExt.log');
    let s;

    before(done => {
      fakeNow = new Date(2012, 8, 12, 10, 37, 11);
      s = new RollingFileWriteStream(fileObj.path, {
        intervalDays: 3,
        maxSize: 5,
        keepFileExt: true,
        compress: true
      });
      const flows = Array.from(Array(8).keys()).map(i => cb => {
        fakeNow = new Date(2012, 8, 12 + parseInt(i / 5, 10), 10, 37, 11);
        s.write(i.toString(), 'utf8', cb);
      });
      async.waterfall(flows, () => done());
    });

    after(done => {
      s.end();
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should rotate with the same extension', () => {
      const files = fs.readdirSync(fileObj.dir);
      const expectedFileList = [
        fileObj.base,
        fileObj.name + '.2012-09-12.1.log.gz'
      ];
      files.length.should.equal(expectedFileList.length);
      files.should.containDeep(expectedFileList);

      fs.readFileSync(path.format(fileObj)).toString().should.equal('567');
      const content = fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-12.1.log.gz'
      })));
      zlib.gunzipSync(content).toString().should.equal('01234');
    });
  });

  describe('with alwaysIncludePattern and keepFileExt', () => {
    const fileObj = generateTestFile('keepFileExt.log');
    let s;

    before(done => {
      fakeNow = new Date(2012, 8, 12, 10, 37, 11);
      s = new RollingFileWriteStream(fileObj.path, {
        intervalDays: 3,
        maxSize: 5,
        keepFileExt: true,
        alwaysIncludePattern: true
      });
      const flows = Array.from(Array(8).keys()).map(i => cb => {
        fakeNow = new Date(2012, 8, 12 + parseInt(i / 5, 10), 10, 37, 11);
        s.write(i.toString(), 'utf8', cb);
      });
      async.waterfall(flows, () => done());
    });

    after(done => {
      s.end();
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should rotate with the same extension and keep date in the filename', () => {
      const files = fs.readdirSync(fileObj.dir);
      const expectedFileList = [
        fileObj.name + '.2012-09-12.log',
        fileObj.name + '.2012-09-12.1.log'
      ];
      files.length.should.equal(expectedFileList.length);
      files.should.containDeep(expectedFileList);
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-12.log'
      }))).toString().should.equal('567');
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-12.1.log'
      }))).toString().should.equal('01234');
    });
  });

  describe('with 5 maxSize, 3 interval days, compress, keepFileExt and alwaysIncludePattern', () => {
    const fileObj = generateTestFile('keepFileExt.log');
    let s;

    before(done => {
      fakeNow = new Date(2012, 8, 12, 10, 37, 11);
      s = new RollingFileWriteStream(fileObj.path, {
        intervalDays: 3,
        maxSize: 5,
        compress: true,
        keepFileExt: true,
        alwaysIncludePattern: true
      });
      const flows = Array.from(Array(38).keys()).map(i => cb => {
        fakeNow = new Date(2012, 8, 12 + parseInt(i / 5, 10), 10, 37, 11);
        s.write(i.toString(), 'utf8', cb);
      });
      async.waterfall(flows, () => done());
    });

    after(done => {
      s.end();
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should rotate after 3 days', () => {
      const files = fs.readdirSync(fileObj.dir);
      const expectedFileList = [
        fileObj.name + '.2012-09-18.log',
        fileObj.name + '.2012-09-12.1.log.gz',
        fileObj.name + '.2012-09-12.2.log.gz',
        fileObj.name + '.2012-09-12.3.log.gz',
        fileObj.name + '.2012-09-12.4.log.gz',
        fileObj.name + '.2012-09-15.1.log.gz',
        fileObj.name + '.2012-09-15.2.log.gz',
        fileObj.name + '.2012-09-15.3.log.gz',
        fileObj.name + '.2012-09-15.4.log.gz',
        fileObj.name + '.2012-09-15.5.log.gz',
        fileObj.name + '.2012-09-18.1.log.gz',
        fileObj.name + '.2012-09-18.2.log.gz'
      ];
      files.length.should.equal(expectedFileList.length);
      files.should.containDeep(expectedFileList);
      fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-18.log',
      }))).toString().should.equal('3637');
      zlib.gunzipSync(fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-12.1.log.gz',
      })))).toString().should.equal('1314');
      zlib.gunzipSync(fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-12.2.log.gz',
      })))).toString().should.equal('101112');
      zlib.gunzipSync(fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-12.3.log.gz',
      })))).toString().should.equal('56789');
      zlib.gunzipSync(fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-12.4.log.gz',
      })))).toString().should.equal('01234');
      zlib.gunzipSync(fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-15.1.log.gz',
      })))).toString().should.equal('272829');
      zlib.gunzipSync(fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-15.2.log.gz',
      })))).toString().should.equal('242526');
      zlib.gunzipSync(fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-15.3.log.gz',
      })))).toString().should.equal('212223');
      zlib.gunzipSync(fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-15.4.log.gz',
      })))).toString().should.equal('181920');
      zlib.gunzipSync(fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-15.5.log.gz',
      })))).toString().should.equal('151617');
      zlib.gunzipSync(fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-18.1.log.gz',
      })))).toString().should.equal('333435');
      zlib.gunzipSync(fs.readFileSync(path.format(_.assign({}, fileObj, {
        base: fileObj.name + '.2012-09-18.2.log.gz',
      })))).toString().should.equal('303132');
    });
  });

  describe('when old files exit', () => {
    const fileObj = generateTestFile();
    let s;

    before(done => {
      fakeNow = new Date(2012, 8, 12, 10, 37, 11);
      fs.ensureFileSync(fileObj.path);
      fs.writeFileSync(fileObj.path, 'exist');
      s = new RollingFileWriteStream(fileObj.path);
      s.write('now', 'utf8', done);
    });

    after(done => {
      s.end();
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should use write in the old file if not reach the maxSize limit', () => {
      const files = fs.readdirSync(fileObj.dir);
      const expectedFileList = [fileObj.base];
      files.length.should.equal(expectedFileList.length);
      files.should.containDeep(expectedFileList);

      fs.readFileSync(path.format(fileObj)).toString().should.equal('existnow');
    });
  });

  describe('when dir does not exists', () => {
    const fileObj = generateTestFile();
    let s;

    before(done => {
      fakeNow = new Date(2012, 8, 12, 10, 37, 11);
      s = new RollingFileWriteStream(fileObj.path);
      s.write('test', 'utf8', done);
    });

    after(done => {
      s.end();
      fs.removeSync(fileObj.dir);
      done();
    });

    it('should use write in the old file if not reach the maxSize limit', () => {
      const files = fs.readdirSync(fileObj.dir);
      const expectedFileList = [fileObj.base];
      files.length.should.equal(expectedFileList.length);
      files.should.containDeep(expectedFileList);

      fs.readFileSync(path.format(fileObj)).toString().should.equal('test');
    });
  });

});
