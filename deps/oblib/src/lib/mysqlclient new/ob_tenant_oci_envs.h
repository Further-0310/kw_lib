#ifndef OBTENANOCIENVS_H
#define OBTENANOCIENVS_H
namespace oceanbase
{
namespace common
{
namespace sqlclient
{
#ifndef OB_BUILD_DBLINK
class ObTenantOciEnvs
{
public:
  static int mtl_init(ObTenantOciEnvs *&tenant_oci_envs) { return OB_SUCCESS; }
  static void mtl_destroy(ObTenantOciEnvs *&tenant_oci_envs) { }
};
#endif

} //sqlclient
} // namespace common
} // namespace oceanbase
#endif //OBTENANOCIENVS_H