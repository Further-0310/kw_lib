ob_set_subtarget(ob_share ALONE
  datum/ob_datum_funcs.cpp
)

ob_set_subtarget(ob_share common_mixed
  datum/ob_datum.cpp
)

ob_add_new_object_target(ob_share ob_share)

target_compile_options(ob_share PRIVATE -Wshorten-64-to-32)
target_link_libraries(ob_share PUBLIC ob_base)

add_library(ob_share_static
  STATIC
  EXCLUDE_FROM_ALL)

target_include_directories(ob_share_static PUBLIC 
  /mnt/kw_lib
  /mnt/kw_lib/src
  /mnt/kw_lib/deps/easy/src
  /mnt/kw_lib/deps/oblib/src
  /mnt/kw_lib/deps/oblib/src/common
  /mnt/kw_lib/deps/easy/src/include)

target_link_libraries(ob_share_static
  PUBLIC easy oblib ob_share)
