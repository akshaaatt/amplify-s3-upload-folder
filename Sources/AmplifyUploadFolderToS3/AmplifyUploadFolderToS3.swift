import Foundation
import Amplify
import Combine

public struct AmplifyUploadFolderToS3 {
    private var subscriptions = Set<AnyCancellable>()

    private func getDocumentsDirectory() -> String {
        let dirPath = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)[0] as String
        return dirPath
    }
    
    private func getRootPath() -> String {
        let dirPath = getDocumentsDirectory()

        let filePath = NSURL(fileURLWithPath: dirPath).path

        return filePath!
    }
    
    func uploadFolderToS3(basePath: String, folderName: String, operation -> Any) {
        let dirPath = getRootPath() + "/" + folderName
        let fileURLs = FileManager.default.listFiles(path: dirPath).filter { !$0.hasDirectoryPath }
        let numOfImages = fileURLs.count

        let uploadQueue = DispatchQueue.global(qos: .userInitiated)
        let uploadGroup = DispatchGroup()
        let uploadSemaphore = DispatchSemaphore(value: 8)

        uploadQueue.async(group: uploadGroup) { [weak self] in
            guard let self = self else { return }

            for (_, fileURL) in fileURLs.enumerated() {
                uploadGroup.enter()
                uploadSemaphore.wait()

                var key = "\(basePath)/\(folderName)/\(fileURL.lastPathComponent)"
                print("\(key) starting to upload")

                let uploadTask = Amplify.Storage.uploadFile(key: key, local: fileURL)

                uploadTask
                    .resultPublisher
                    .receive(on: uploadQueue)
                    .sink {
                        if case let .failure(storageError) = $0 {
                            print("Failed: \(storageError.errorDescription). \(storageError.recoverySuggestion)")
                        }
                        uploadGroup.leave()
                        uploadSemaphore.signal()
                    }
                    receiveValue: { data in
                        print("Completed: \(data)")
                    }
                    .store(in: &self.subscriptions)
            }
        }

        uploadGroup.notify(queue: uploadQueue) {
            operation()
        }
    }
}
