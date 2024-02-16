ob_set_subtarget(ob_storage kw_blocksstable
  blocksstable/kw/kw_data_buffer.cpp
  blocksstable/kw/kw_datum_row.cpp
  blocksstable/kw/kw_datum_rowkey.cpp
  blocksstable/kw/kw_imicro_block_reader.cpp
  blocksstable/kw/kw_imicro_block_writer.cpp
  blocksstable/kw/kw_macro_block_common_header.cpp
  blocksstable/kw/kw_macro_block_reader.cpp
  blocksstable/kw/kw_macro_block_writer.cpp
  blocksstable/kw/kw_macro_block.cpp
  # blocksstable/kw/kw_micro_block_encoder.cpp
  blocksstable/kw/kw_micro_block_header.cpp
  blocksstable/kw/kw_micro_block_reader.cpp
  blocksstable/kw/kw_micro_block_writer.cpp
  blocksstable/kw/kw_row_reader.cpp
  blocksstable/kw/kw_row_writer.cpp
  blocksstable/kw/kw_sstable_macro_block_header.cpp
  blocksstable/kw/kw_sstable_printer.cpp
  blocksstable/kw/kw_table_param.cpp
  blocksstable/kw/kw_table_read_info.cpp
)

ob_add_new_object_target(ob_storage ob_storage)

target_link_libraries(ob_storage PUBLIC ob_base)

add_library(ob_storage_static
  STATIC
  EXCLUDE_FROM_ALL)

target_include_directories(ob_storage_static PUBLIC 
  /mnt/kw_lib
  /mnt/kw_lib/src
  /mnt/kw_lib/deps/easy/src
  /mnt/kw_lib/deps/oblib/src
  /mnt/kw_lib/deps/oblib/src/common
  /mnt/kw_lib/deps/easy/src/include)

target_link_libraries(ob_storage_static
  PUBLIC easy oblib ob_storage)

