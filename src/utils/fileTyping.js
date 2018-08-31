const getFileTypeFromName = function(filename) {
  if(typeof filename !== 'string') return undefined;
  return filename.toLowerCase().split('.').pop();
};

module.exports = {
  getFileTypeFromName
};