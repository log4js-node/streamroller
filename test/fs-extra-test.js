const should = require("should");
const path = require("path");

describe("simulate fs monkey-patch", function() {
  const fs = require("fs");
  const realpathNativeBackup = fs.realpath.native;

  beforeEach(function() {
    // simulate fs monkey-patch
    delete fs.realpath.native;
  });

  afterEach(function() {
    // reset require.cache
    delete require.cache[require.resolve("../lib/RollingFileWriteStream")];
    delete require.cache[require.resolve("../lib/moveAndMaybeCompressFile")];
    const fsePath = path.dirname(require.resolve("fs-extra"));
    for (const entry in require.cache) {
      if (entry.startsWith(fsePath)) {
        delete require.cache[entry]
      }
    }
    // reinstate fs.realpath.native
    fs.realpath.native = realpathNativeBackup;
  });

  it("RollingFileWriteStream should not throw error", function() {
    console.log(Object.keys(require.cache).length);
    should.doesNotThrow(function() {
      require("../lib/RollingFileWriteStream");
    });
    console.log(Object.keys(require.cache).length);
  });

  it("moveAndMaybeCompressFile should not throw error", function() {
    console.log(Object.keys(require.cache).length);
    should.doesNotThrow(function() {
      require("../lib/moveAndMaybeCompressFile");
    });
    console.log(Object.keys(require.cache).length);
  });
});