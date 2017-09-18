#include "blobfuse.h"

int azs_mkdir(const char *path, mode_t mode)
{
	if (AZS_PRINT)
	{
		fprintf(stdout, "mkdir called with path = %s\n", path);
	}

	std::string pathstr(path);
	std::string blobNameStr = pathstr.substr(1);
	while (true)
	{
		file_info_map_mutex.lock();
		auto entry = file_info_map.find(blobNameStr);
		if (entry == file_info_map.end())
		{
			std::shared_ptr<struct file_status> ptr = std::make_shared<struct file_status>(0, 0, 0, true);
			ptr->cache_update_mutex.lock();
			file_info_map[blobNameStr] = ptr;
			file_info_map_mutex.unlock();
			
			blobNameStr.push_back('/');
			if (list_one_blob_hierarchical(str_options.containerName, "/", blobNameStr))
			{
				ensure_files_directory_exists(prepend_mnt_path_string(pathstr + "/placeholder"));
				ptr->state.store(3);
				ptr->cache_update_mutex.unlock();
				return -EEXIST;
			}
			else
			{
				blobNameStr.insert(blobNameStr.size(), directorySignifier);

				std::istringstream emptyDataStream("");
				std::vector<std::pair<std::string, std::string>> metadata;
				errno = 0;
				azure_blob_client_wrapper->upload_block_blob_from_stream(str_options.containerName, blobNameStr, emptyDataStream, metadata);
				// TODO: Use an access condition to ensure that we actually create the directory.  Fail if the we could not create the directory.
				if (errno != 0)
				{
					int errno_val = errno;
					ptr->state.store(5);
					ptr->cache_update_mutex.unlock();
					std::lock_guard<std::mutex> lock(file_info_map_mutex);
					file_info_map.erase(pathstr.substr(1));
					
					return 0 - map_errno(errno_val);
				}
				else
				{
					ensure_files_directory_exists(prepend_mnt_path_string(pathstr + "/placeholder"));
					ptr->state.store(3);
					ptr->cache_update_mutex.unlock();
					return 0;
				}
			}
		}
		else
		{
			file_info_map_mutex.unlock();
			int curstate = entry->second->state.load();
			if (curstate == 3)
			{
				return -EEXIST;
			}
			std::lock_guard<std::mutex> lock(entry->second->cache_update_mutex);
			if (curstate == 3)
			{
				return -EEXIST;
			}
			else
			{
				continue;
			}
		}
	}

//	return 0;
}

/**
 * Read the contents of a directory.  For each entry to add, call the filler function with the input buffer,
 * the name of the entry, and additional data about the entry.  TODO: Keep the data (somehow) for latter getattr calls.
 *
 * @param  path   Path to the directory to read.
 * @param  buf    Buffer to pass into the filler function.  Not otherwise used in this function.
 * @param  filler Function to call to add directories and files as they are discovered.
 * @param  offset Not used
 * @param  fi     File info about the directory to be read.
 * @param  flags  Not used.  TODO: Consider prefetching on FUSE_READDIR_PLUS.
 * @return        TODO: error codes.
 */
int azs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	if (AZS_PRINT)
	{
		fprintf(stdout, "azs_readdir called with path = %s\n", path);
	}
	std::string pathStr(path);
	if (pathStr.size() > 1)
	{
		pathStr.push_back('/');
	}

	errno = 0;
	std::vector<list_blobs_hierarchical_item> listResults = list_all_blobs_hierarchical(str_options.containerName, "/", pathStr.substr(1));
	if (errno != 0)
	{
		return 0 - map_errno(errno);
	}

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	int i = 0;
	if (AZS_PRINT)
	{
		fprintf(stdout, "result count = %lu\n", listResults.size());
	}

	std::lock_guard<std::mutex> lock(file_info_map_mutex);
	for (; i < listResults.size(); i++)
	{
		int fillerResult;
		// We need to parse out just the trailing part of the path name.
		int len = listResults[i].name.size();
		// TODO: simplify this tokenization logic - we already know the path to the directory.
		if (len > 0)
		{
			char *nameCopy = (char *)malloc(len + 1);
			memcpy(nameCopy, listResults[i].name.c_str(), len);
			nameCopy[len] = 0;

			char *lasts = NULL;
			char *token = strtok_r(nameCopy, "/", &lasts);
			char *prevtoken = NULL;

			while (token)
			{
				prevtoken = token;
				token = strtok_r(NULL, "/", &lasts);
			}

			if (!listResults[i].is_directory)
			{
				if (prevtoken && (strcmp(prevtoken, directorySignifier.c_str()) != 0))
				{
					if (file_info_map.find(listResults[i].name) == file_info_map.end())
					{
						file_info_map[listResults[i].name] = std::make_shared<file_status>(1, listResults[i].content_length, 0, false);
					}

					struct stat stbuf;
					stbuf.st_mode = S_IFREG | 0777; // Regular file (not a directory)
					stbuf.st_nlink = 1;
					stbuf.st_size = listResults[i].content_length;
					fillerResult = filler(buf, prevtoken, &stbuf, 0); // TODO: Add stat information.  Consider FUSE_FILL_DIR_PLUS.
					if (AZS_PRINT)
					{
						fprintf(stdout, "blob result = %s, fillerResult = %d\n", prevtoken, fillerResult);
					}
				}

			}
			else
			{
				if (prevtoken)
				{
					if (file_info_map.find(listResults[i].name) == file_info_map.end())
					{
						file_info_map[listResults[i].name] = std::make_shared<file_status>(0, 0, 0, true);
					}
					struct stat stbuf;
					stbuf.st_mode = S_IFDIR | 0777;
					stbuf.st_nlink = 2;
					fillerResult = filler(buf, prevtoken, &stbuf, 0);
					if (AZS_PRINT)
					{
						fprintf(stdout, "dir result = %s, fillerResult = %d\n", prevtoken, fillerResult);
					}
				}

			}


			free(nameCopy);
		}

	}
	if (AZS_PRINT)
	{
		fprintf(stdout, "Done with readdir\n");
	}
	return 0;

}

int azs_rmdir(const char *path)
{
	if (AZS_PRINT)
	{
		fprintf(stdout, "azs_rmdir called with path = %s\n", path);
	}

	
	std::string pathstr(path);
	std::string blobNameStr = pathstr.substr(1);

	file_info_map_mutex.lock();
	auto entry = file_info_map.find(blobNameStr);
	if (entry == file_info_map.end())
	{
		std::shared_ptr<struct file_status> ptr = std::make_shared<struct file_status>(0, 0, 0, true);
		ptr->cache_update_mutex.lock();
		file_info_map[blobNameStr] = ptr;
		file_info_map_mutex.unlock();
		
		blobNameStr.push_back('/');
		int dirStatus = is_directory_empty(str_options.containerName, "/", blobNameStr);
		if (dirStatus == 0)
		{
			ptr->state.store(5);
			ptr->cache_update_mutex.unlock();
			std::lock_guard<std::mutex> lock(file_info_map_mutex);
			file_info_map.erase(pathstr.substr(1));
			return -ENOENT;
		}
		else if (dirStatus == 2)
		{
			ensure_files_directory_exists(prepend_mnt_path_string(pathstr + "/placeholder"));
			ptr->state.store(3);
			ptr->cache_update_mutex.unlock();
			return -ENOTEMPTY;
		}
		else 
		{
			// dirStatus == 1
			ptr->state.store(5);
			std::string pathString(path);
			const char * mntPath;
			std::string mntPathString = prepend_mnt_path_string(pathString);
			mntPath = mntPathString.c_str();
			if (AZS_PRINT)
			{
				fprintf(stdout, "deleting directory %s\n", mntPath);
			}
			errno = 0;
			int ret = remove(mntPath);
			if (ret != 0)
			{
				int errno_val = errno;
				ptr->state.store(3);
				ptr->cache_update_mutex.unlock();
				return -errno_val;
			}
			
			blobNameStr.insert(blobNameStr.size(), directorySignifier);
			errno = 0;
			azure_blob_client_wrapper->delete_blob(str_options.containerName, blobNameStr);
			if (errno != 0)
			{
				int errno_val = errno;
				ptr->state.store(5);
				ptr->cache_update_mutex.unlock();
				std::lock_guard<std::mutex> lock(file_info_map_mutex);
				file_info_map.erase(pathstr.substr(1));
					
				return 0 - map_errno(errno_val);
			}
			
			ptr->cache_update_mutex.unlock();
			std::lock_guard<std::mutex> lock(file_info_map_mutex);
			file_info_map.erase(pathstr.substr(1));
			return 0;			
		}
	}
	else
	{
		file_info_map_mutex.unlock();
		std::lock_guard<std::mutex> lock(entry->second->cache_update_mutex);
		int state = entry->second->state.load();
		if (state == 5)
		{
			return -ENOENT;
		}
		blobNameStr.push_back('/');
		int dirStatus = is_directory_empty(str_options.containerName, "/", blobNameStr);
		if (dirStatus == 2)
		{
			return -ENOTEMPTY;
		}
		else
		{
			// dirstatus == 1.  In this instance, dirStatus == 0 should be impossible.
			std::string pathString(path);
			const char * mntPath;
			std::string mntPathString = prepend_mnt_path_string(pathString);
			mntPath = mntPathString.c_str();
			if (AZS_PRINT)
			{
				fprintf(stdout, "deleting directory %s\n", mntPath);
			}
			errno = 0;
			int ret = remove(mntPath);
			if (ret != 0)
			{
				int errno_val = errno;
				entry->second->state.store(3);
				entry->second->cache_update_mutex.unlock();
				return -errno_val;
			}
			
			blobNameStr.insert(blobNameStr.size(), directorySignifier);
			errno = 0;
			azure_blob_client_wrapper->delete_blob(str_options.containerName, blobNameStr);
			if (errno != 0)
			{
				int errno_val = errno;
				entry->second->state.store(5);
				entry->second->cache_update_mutex.unlock();
				std::lock_guard<std::mutex> lock(file_info_map_mutex);
				file_info_map.erase(pathstr.substr(1));
					
				return 0 - map_errno(errno_val);
			}
			
			entry->second->cache_update_mutex.unlock();
			std::lock_guard<std::mutex> lock(file_info_map_mutex);
			file_info_map.erase(pathstr.substr(1));
			return 0;
		}
	}

//	return 0;



	/*	errno = 0;
		std::vector<list_blobs_hierarchical_item> listResults = list_all_blobs_hierarchical(str_options.containerName, "/", pathStr.substr(1));
		if (errno != 0)
		{
			return 0 - map_errno(errno);
		}

		int i = 0;
		if (AZS_PRINT)
		{
			fprintf(stdout, "result count = %d\n", listResults.size());
		}
		for (; i < listResults.size(); i++)
		{
			if (!listResults[i].is_directory)
			{
				std::string path_to_blob(listResults[i].name);
				path_to_blob.insert(0, 1, '/');
				int res = azs_unlink(path_to_blob.c_str());
				if (res < 0)
				{
					return res;
				}

			}
			else
			{
				std::string path_to_blob(listResults[i].name);
				path_to_blob.insert(0, 1, '/');
				int res = azs_rmdir(path_to_blob.c_str());
				if (res < 0)
				{
					return res;
				}
			}
		}

		std::string pathString(path);
		const char * mntPath;
		std::string mntPathString = prepend_mnt_path_string(pathString);
		mntPath = mntPathString.c_str();
		if (AZS_PRINT)
		{
			fprintf(stdout, "deleting file %s\n", mntPath);
		}
		remove(mntPath);
		

	return 0;*/

}
