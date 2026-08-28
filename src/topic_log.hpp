#pragma once

#include "record.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace kafka
{
    namespace detail
    {
        // RAII guard to guarantee file descriptor closure on scope exit or exception unwinding
        struct FileDescriptorGuard
        {
            int fd = -1;

            ~FileDescriptorGuard()
            {
                if (fd >= 0)
                {
                    ::close(fd);
                }
            }
        };
    } // namespace detail

    // TopicLog manages appending binary records to a single topic's append-only log file.
    class TopicLog
    {
    public:
        explicit TopicLog(std::string topic, std::string data_dir = "data")
            : topic_(std::move(topic)), data_dir_(std::move(data_dir))
        {
            if (topic_.empty())
            {
                throw std::invalid_argument("topic name cannot be empty");
            }
            log_path_ = data_dir_ + "/" + topic_ + ".log";
        }

        const std::string &topic() const { return topic_; }
        const std::string &log_path() const { return log_path_; }

        // Append a single record to the topic's log file using POSIX file APIs with fsync.
        void append(const Record &record)
        {
            ensure_directory_exists();

            std::string serialized = serialize_record(record);

            // 1. Open topic log file in append mode using POSIX open()
            int fd = -1;
            do
            {
                fd = ::open(log_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            } while (fd < 0 && errno == EINTR);

            if (fd < 0)
            {
                int saved_errno = errno;
                throw std::runtime_error("failed to open topic log file for append (" + log_path_ + "): " + std::strerror(saved_errno));
            }

            detail::FileDescriptorGuard fd_guard{fd};

            // 2. Loop write() to handle potential partial writes and EINTR interrupts
            std::size_t total_written = 0;
            const char *buffer = serialized.data();
            std::size_t total_bytes = serialized.size();

            while (total_written < total_bytes)
            {
                ssize_t bytes_written = ::write(fd_guard.fd, buffer + total_written, total_bytes - total_written);
                if (bytes_written < 0)
                {
                    if (errno == EINTR)
                    {
                        continue;
                    }
                    int saved_errno = errno;
                    throw std::runtime_error("failed to write record to topic log file (" + log_path_ + "): " + std::strerror(saved_errno));
                }
                if (bytes_written == 0)
                {
                    throw std::runtime_error("write returned 0 while appending to topic log file (" + log_path_ + ")");
                }
                total_written += static_cast<std::size_t>(bytes_written);
            }

            // 3. Force flushing dirty pages to physical disk via fsync()
            int fsync_res = -1;
            do
            {
                fsync_res = ::fsync(fd_guard.fd);
            } while (fsync_res < 0 && errno == EINTR);

            if (fsync_res < 0)
            {
                int saved_errno = errno;
                throw std::runtime_error("fsync failed for topic log file (" + log_path_ + "): " + std::strerror(saved_errno));
            }

            // 4. Explicitly close file descriptor and disarm RAII guard
            int close_res = ::close(fd_guard.fd);
            fd_guard.fd = -1;
            if (close_res < 0)
            {
                int saved_errno = errno;
                throw std::runtime_error("failed to close topic log file (" + log_path_ + "): " + std::strerror(saved_errno));
            }
        }

        // Read all complete records from the topic's log file.
        std::vector<Record> read_all() const
        {
            std::vector<Record> records;

            std::error_code ec;
            if (!std::filesystem::exists(log_path_, ec))
            {
                return records;
            }

            std::ifstream file(log_path_, std::ios::binary);
            if (!file.is_open())
            {
                throw std::runtime_error("failed to open topic log file for reading: " + log_path_);
            }

            std::string buffer((std::istreambuf_iterator<char>(file)),
                               std::istreambuf_iterator<char>());
            file.close();

            std::size_t buffer_offset = 0;
            Record record;
            while (deserialize_record(buffer, buffer_offset, record))
            {
                records.push_back(record);
            }

            return records;
        }

    private:
        void ensure_directory_exists() const
        {
            std::error_code ec;
            std::filesystem::create_directories(data_dir_, ec);
            if (ec)
            {
                throw std::runtime_error("failed to create data directory (" + data_dir_ + "): " + ec.message());
            }
        }

        std::string topic_;
        std::string data_dir_;
        std::string log_path_;
    };

} // namespace kafka
