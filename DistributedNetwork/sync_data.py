import os
import shutil

def sync_files(source_dir, dest_dir):
    for dirpath, dirnames, filenames in os.walk(source_dir):
        # Get the relative path to mirror the folder structure
        rel_path = os.path.relpath(dirpath, source_dir)
        target_dir = os.path.join(dest_dir, rel_path)

        os.makedirs(target_dir, exist_ok=True)

        for filename in filenames:
            src_file = os.path.join(dirpath, filename)
            dst_file = os.path.join(target_dir, filename)

            # Copy if file doesn't exist or is older
            if (not os.path.exists(dst_file)) or (
                os.path.getmtime(src_file) > os.path.getmtime(dst_file)
            ):
                print(f"Copying {src_file} -> {dst_file}")
                shutil.copy2(src_file, dst_file)

# Define source and destination paths
source = r'C:\AAMS_data\DistributedNetwork'
dest = r'W:\ESG\DOW_MAQ\MAQ_Archive\AAMS_archive\DistributedNetwork'

sync_files(source, dest)
