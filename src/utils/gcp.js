class GoogleCloudPlatform {
  constructor(storageObj) {
    this.storage = storageObj;
  }

  createBucket(bucketName) {
    //promise
    return this.storage.createBucket(bucketName);
  }
  getBucket(bucketName) {
    return this.storage.bucket(bucketName);
  }
}

module.exports = GoogleCloudPlatform;