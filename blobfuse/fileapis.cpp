#include "blobfuse.h"
#include <algorithm>

int azs_open(const char *path, struct fuse_file_info *fi)
{
	if (AZS_PRINT)
	{
		fprintf(stdout, "azs_open called with path = %s, fi->flags = %X. \n", path, fi->flags);
	}
	std::string pathString(path);
	std::string blobString = pathString.substr(1);
	
	while (true)
	{
		file_info_map_mutex.lock();
		auto entry = file_info_map.find(blobString);
		if (entry == file_info_map.end())
		{
			std::shared_ptr<struct file_status> ptr = std::make_shared<struct file_status>(0, 0, 0, false);
			ptr->cache_update_mutex.lock();
			file_info_map[blobString] = ptr;
			file_info_map_mutex.unlock();

			std::string mntPathString = prepend_mnt_path_string(pathString);
			ensure_files_directory_exists(mntPathString);
			std::ofstream filestream(mntPathString, std::ofstream::binary | std::ofstream::out);
			errno = 0;
			azure_blob_client_wrapper->download_blob_to_stream(str_options.containerName, blobString, 0ULL, 1000000000000ULL, filestream);
			if (errno != 0)
			{
				int errno_ret = errno;
				ptr->state.store(5);
				ptr->cache_update_mutex.unlock();
				std::lock_guard<std::mutex> lock(file_info_map_mutex);
				file_info_map.erase(blobString);
				return 0 - map_errno(errno_ret);
			}
			
			ptr->state.store(3);
			ptr->open_handle_count++;
			
			struct stat buf;
			int statret = stat(mntPath, &buf);
			ptr->size.store(buf.st_size);
			ptr->cache_update_mutex.unlock();
			
			errno = 0;
			int res;

			res = open(mntPath, fi->flags);
			if (AZS_PRINT)
			{
				printf("Accessing %s gives res = %d, errno = %d, ENOENT = %d\n", mntPath, res, errno, ENOENT);
			}

			if (res == -1)
			{
				entry->second->open_handle_count--;
				return -errno;
			}
			fchmod(res, 0777);
			struct fhwrapper *fhwrap = new fhwrapper(res, ((fi->flags & O_WRONLY == O_WRONLY) || (fi->flags & O_RDWR == O_RDWR)), ptr);
			fi->fh = (long unsigned int)fhwrap;
			return 0;
		}
		else
		{
			file_info_map_mutex.unlock();
			entry->second->cache_update_mutex.lock();
			int state = entry->second->state.load();
			if (state == 5)
			{
				entry->second->cache_update_mutex.unlock();
				continue;
			}
			else 
			{	
				if (state == 3)
				{
					struct stat buf;
					int statret = stat(mntPath, &buf);
					if (entry->second->open_handle_count.load() != 0 || ((statret == 0) && ((time(NULL) - buf.st_atime) < 12000)))
					{
						entry->second->open_handle_count++;
						entry->second->cache_update_mutex.unlock();
						errno = 0;
						int res;

						res = open(mntPath, fi->flags);
						if (AZS_PRINT)
						{
							printf("Accessing %s gives res = %d, errno = %d, ENOENT = %d\n", mntPath, res, errno, ENOENT);
						}

						if (res == -1)
						{
							entry->second->open_handle_count--;
							return -errno;
						}
						fchmod(res, 0777);
						struct fhwrapper *fhwrap = new fhwrapper(res, ((fi->flags & O_WRONLY == O_WRONLY) || (fi->flags & O_RDWR == O_RDWR)), entry->second);
						fi->fh = (long unsigned int)fhwrap;
						return 0;
					}
				}
				
				if (state == 1)
				{
					entry->second->state.store(2);
					
					std::string mntPathString = prepend_mnt_path_string(pathString);
					ensure_files_directory_exists(mntPathString);
					std::ofstream filestream(mntPathString, std::ofstream::binary | std::ofstream::out);
					errno = 0;
					azure_blob_client_wrapper->download_blob_to_stream(str_options.containerName, blobString, 0ULL, 1000000000000ULL, filestream);
					if (errno != 0)
					{
						int errno_ret = errno;
						entry->second->state.store(1);
						entry->second->cache_update_mutex.unlock();
						return 0 - map_errno(errno_ret);
					}
					
					entry->second->state.store(3);
					entry->second->open_handle_count++;
					struct stat buf;
					int statret = stat(mntPath, &buf);
					entry->second->size.store(buf.st_size);
					entry->second->cache_update_mutex.unlock();
					
					errno = 0;
					int res;

					res = open(mntPath, fi->flags);
					if (AZS_PRINT)
					{
						printf("Accessing %s gives res = %d, errno = %d, ENOENT = %d\n", mntPath, res, errno, ENOENT);
					}

					if (res == -1)
					{
						entry->second->open_handle_count--;
						return -errno;
					}
					fchmod(res, 0777);
					struct fhwrapper *fhwrap = new fhwrapper(res, ((fi->flags & O_WRONLY == O_WRONLY) || (fi->flags & O_RDWR == O_RDWR)), entry->second);
					fi->fh = (long unsigned int)fhwrap;
					return 0;
				}
			}
		}
	}
}

/**
 * Read data from the file (the blob) into the input buffer
 * @param  path   Path of the file (blob) to read from
 * @param  buf    Buffer in which to copy the data
 * @param  size   Amount of data to copy
 * @param  offset Offset in the file (the blob) from which to begin reading.
 * @param  fi     File info for this file.
 * @return        TODO: Error codes
 */
int azs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	std::string pathString(path);
	const char * mntPath;
	std::string mntPathString = prepend_mnt_path_string(pathString);
	mntPath = mntPathString.c_str();
	int fd;
	int res;

	(void) fi;
	if (fi == NULL)
		fd = open(mntPath, O_RDONLY);
	else
		fd = ((struct fhwrapper *)fi->fh)->fh;

	if (fd == -1)
		return -errno;

	res = pread(fd, buf, size, offset);
	if (res == -1)
		res = -errno;

	if (fi == NULL)
		close(fd);
	return res;
}

int azs_create_parent_dir_exists(std::string pathString, mode_t mode, struct fuse_file_info *fi)
{
	int state = ptr->state.load();
	if (state != 3)
	{
		std::lock_guard<std::mutex> lock(ptr->cache_update_mutex);
	}
	state = ptr->state.load();
	if (state == 5)
	{
		return 1;
	}
	
	file_info_map_mutex.lock();
	auto entry = file_info_map.find(pathString.substr(1));
	if (entry == file_info_map.end())
	{
		std::shared_ptr<struct file_status> ptr = std::make_shared<struct file_status>(3, 0, 1, false);
		ptr->cache_update_mutex.lock();
		file_info_map[pathString.substr(1)] = ptr;
		file_info_map_mutex.unlock();
		
		const char * mntPath;
		std::string mntPathString = prepend_mnt_path_string(pathString);
		ensure_files_directory_exists(mntPathString);
		int res = open(mntPath, fi->flags, mode);
		if (AZS_PRINT)
		{
			fprintf(stdout, "mntPath = %s, result = %d\n", mntPath, res);
		}
		if (res == -1)
		{
			int errno_res = errno;
			if (AZS_PRINT)
			{
				fprintf(stdout, "Error in open, errno = %d\n", errno);
			}
			ptr->state.store(5);
			ptr->cache_update_mutex.unlock();
			
			std::lock_guard<std::mutex> lock(file_info_map_mutex);
			file_info_map.erase(pathString.substr(1));
			return -errno_res;
		}

		ptr->cache_update_mutex.unlock();
		struct fhwrapper *fhwrap = new fhwrapper(res, true, ptr);
		fi->fh = (long unsigned int)fhwrap;
		return 0;
	}
	else
	{
		file_info_map_mutex.unlock();
		entry->second->cache_update_mutex.lock();
		const char * mntPath;
		std::string mntPathString = prepend_mnt_path_string(pathString);
		ensure_files_directory_exists(mntPathString);
		int res = open(mntPath, fi->flags, mode);
		if (AZS_PRINT)
		{
			fprintf(stdout, "mntPath = %s, result = %d\n", mntPath, res);
		}
		if (res == -1)
		{
			int errno_res = errno;
			if (AZS_PRINT)
			{
				fprintf(stdout, "Error in open, errno = %d\n", errno);
			}
			entry->second->cache_update_mutex.unlock();
			
			std::lock_guard<std::mutex> lock(file_info_map_mutex);
			file_info_map.erase(pathString.substr(1));
			return -errno_res;
		}
		
		entry->second->size.store(0);
		entry->second->open_handle_count++;
		entry->second->cache_update_mutex.unlock();
		struct fhwrapper *fhwrap = new fhwrapper(res, true, entry->second);
		fi->fh = (long unsigned int)fhwrap;
		return 0;
	}
}

int azs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	if (AZS_PRINT)
	{
		fprintf(stdout, "azs_create called with path = %s, mode = %d, fi->flags = %x\n", path, mode, fi->flags);
	}
	
	std::string pathString(path);
	size_t last_slash_index = pathString.find_last_of('/');
	
	while (true)
	{
		if (last_slash_index == 0)
		{
			return azs_create_parent_dir_exists(pathString, mode, fi, nullptr);
		}
		
		std::string dirString = pathString.substr(0, last_slash_index);
		file_info_map_mutex.lock();
		dirBlobNameStr = dirString.substr(1);
		auto entry = file_info_map.find(dirBlobNameStr);
		if (entry != file_info_map.end())
		{
			file_info_map_mutex.unlock();
			
			int state = ptr->state.load();
			if (state != 3)
			{
				std::lock_guard<std::mutex> lock(entry->second->cache_update_mutex);
			}
			state = ptr->state.load();
			if (state == 5)
			{
				continue;
			}
		
			return azs_create_parent_dir_exists(pathString, mode, fi, entry->second);
		}
		
		// Parent directory does not exist in the cache.
		
		std::shared_ptr<struct file_status> dirptr = std::make_shared<struct file_status>(0, 0, 0, true);
		dirptr->cache_update_mutex.lock();
		file_info_map[dirBlobNameStr] = dirptr;
		file_info_map_mutex.unlock();
		
		dirBlobNameStr.push_back('/');
		if (!list_one_blob_hierarchical(str_options.containerName, "/", blobNameStr))
		{
			dirptr->state.store(5);
			dirptr->cache_update_mutex.unlock();
			std::lock_guard<std::mutex> lock(file_info_map_mutex);
			file_info_map.erase(dirString.substr(1));
			return -ENOENT;
		}
		else
		{
			std::string mntPathString = prepend_mnt_path_string(pathString);
			ensure_files_directory_exists(mntPathString);
			dirptr->state.store(3);
			dirptr->cache_update_mutex.unlock();
			return azs_create_parent_dir_exists(pathString, mode, fi, nullptr);
		}
	}
}

/**
 * Write data to the file.
 *
 * Here, we are still just writing data to the local buffer, not forwarding to Storage.
 * TODO: possible in-memory caching?
 * TODO: for very large files, start uploading to Storage before all the data has been written here.
 * @param  path   Path to the file to write.
 * @param  buf    Buffer containing the data to write.
 * @param  size   Amount of data to write
 * @param  offset Offset in the file to write the data to
 * @param  fi     Fuse file info, containing the fh pointer
 * @return        TODO: Error codes.
 */
int azs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	std::string pathString(path);
	const char * mntPath;
	std::string mntPathString = prepend_mnt_path_string(pathString);
	mntPath = mntPathString.c_str();
	int fd;
	int res;

	(void) fi;
	if (fi == NULL)
		fd = open(mntPath, O_WRONLY);
	else
		fd = ((struct fhwrapper *)fi->fh)->fh;

	if (fd == -1)
		return -errno;

	res = pwrite(fd, buf, size, offset);
	if (res == -1)
		res = -errno;
	
	while (true)
	{
		unsigned long long cur_size = ((struct fhwrapper *)fi->fh)->ptr->size.load();
		unsigned long long max_written = offset + res;
		if (max_written > cur_size)
		{
			if (((struct fhwrapper *)fi->fh)->ptr->size.compare_exchange_strong(&cur_size, max_written)
			{
				break;
			}
		}
		else
		{
			break;
		}
	}
	
	return res;
}

int azs_flush(const char *path, struct fuse_file_info *fi)
{
	if (AZS_PRINT)
	{
		fprintf(stdout, "azs_flush called with path = %s, fi->flags = %d\n", path, fi->flags);
	}
	std::string pathString(path);
	const char * mntPath;
	std::string mntPathString = prepend_mnt_path_string(pathString);
	mntPath = mntPathString.c_str();
	if (AZS_PRINT)
	{
		fprintf(stdout, "Now accessing %s.\n", mntPath);
	}
	if (access(mntPath, F_OK) != -1 )
	{
		close(dup(((struct fhwrapper *)fi->fh)->fh));
		if (((struct fhwrapper *)fi->fh)->upload)
		{
			// TODO: This will currently upload the full file on every flush() call.  We may want to keep track of whether
			// or not flush() has been called already, and not re-upload the file each time.
			
			std::lock_guard<std::mutex> lock(((struct fhwrapper *)fi->fh)->ptr->cache_update_mutex);
			if (((struct fhwrapper *)fi->fh)->ptr->state.load() != 3)
			{
				return -EINVAL; // Indicates a bug in the code somewhere.
			}
			
			((struct fhwrapper *)fi->fh)->ptr->state.store(4);
			
			std::vector<std::pair<std::string, std::string>> metadata;
			errno = 0;
			azure_blob_client_wrapper->upload_file_to_blob(mntPath, str_options.containerName, pathString.substr(1), metadata, 8);
			((struct fhwrapper *)fi->fh)->ptr->state.store(3);
			if (errno != 0)
			{
				return 0 - map_errno(errno);
			}
		}
	}

	return 0;
}


int azs_release(const char *path, struct fuse_file_info * fi)
{
	if (AZS_PRINT)
	{
		fprintf(stdout, "azs_release called with path = %s, fi->flags = %d\n", path, fi->flags);
	}
	std::string pathString(path);
	const char * mntPath;
	std::string mntPathString = prepend_mnt_path_string(pathString);
	mntPath = mntPathString.c_str();
	if (AZS_PRINT)
	{
		fprintf(stdout, "Now accessing %s.\n", mntPath);
	}
	if (access(mntPath, F_OK) != -1 )
	{
		close(((struct fhwrapper *)fi->fh)->fh);
		((struct fhwrapper *)fi->fh)->ptr->open_handle_count--;
	}
	else
	{
		if (AZS_PRINT)
		{
			fprintf(stdout, "Access failed.\n");
		}
	}
	return 0;
}

int azs_unlink(const char *path)
{
	std::string pathString(path);
	std::string blobString = pathString.substr(1);
	
	file_info_map_mutex.lock();
	auto entry = file_info_map.find(blobString);
	if (entry == file_info_map.end())
	{
		std::shared_ptr<struct file_status> ptr = std::make_shared<struct file_status>(5, 0, 0, false);
		ptr->cache_update_mutex.lock();
		file_info_map[blobString] = ptr;
		file_info_map_mutex.unlock();
		
		errno = 0;
		azure_blob_client_wrapper->delete_blob(str_options.containerName, blobString);
		if (errno != 0)
		{
			int errno_ret = errno;
			ptr->cache_update_mutex.unlock();
			std::lock_guard<std::mutex> lock(file_info_map_mutex);
			file_info_map.erase(blobString);
			return 0 - map_errno(errno_ret);
		}
		
		ptr->cache_update_mutex.unlock();
		std::lock_guard<std::mutex> lock(file_info_map_mutex);
		file_info_map.erase(blobString);
		return 0;
	}
	else
	{
		file_info_map_mutex.unlock();
		entry->second->cache_update_mutex.lock();
		int open_handle_count = entry->second->open_handle_count.load();
		if (open_handle_count > 0)
		{
			entry->second->cache_update_mutex.unlock();
			sleep(3);
			entry->second->cache_update_mutex.lock();
			open_handle_count = entry->second->open_handle_count.load();
			if (open_handle_count > 0)
			{
				entry->second->cache_update_mutex.unlock();
				return -EBUSY;
			}
		}
		int state = entry->second->state.load();
		if (state == 5)
		{
			entry->second->cache_update_mutex.unlock();
			return -ENOENT;
		}
		
		entry->second->state.store(5);
		
		std::string mntPathString = prepend_mnt_path_string(pathString);
		remove(mntPathString.c_str());
		
		errno = 0;
		azure_blob_client_wrapper->delete_blob(str_options.containerName, blobString);
		if (errno != 0)
		{
			int errno_ret = errno;
			entry->second->state.store(state);
			entry->second->cache_update_mutex.unlock();
			return 0 - map_errno(errno_ret);
		}
		
		entry->second->cache_update_mutex.unlock();
		std::lock_guard<std::mutex> lock(file_info_map_mutex);
		file_info_map.erase(blobString);
		return 0;
	}
}