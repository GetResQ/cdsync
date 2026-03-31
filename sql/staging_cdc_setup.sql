\set ON_ERROR_STOP on

begin;

do $$
begin
  if not exists (
    select 1 from pg_roles where rolname = 'cdsync_staging'
  ) then
    create role cdsync_staging
      login
      password '__CDSYNC_PASSWORD__';
  else
    alter role cdsync_staging
      with login
      password '__CDSYNC_PASSWORD__';
  end if;
end
$$;

grant rds_replication to cdsync_staging;

grant connect on database resq to cdsync_staging;
grant usage on schema public to cdsync_staging;

grant select on table
  public.workorders_workorder,
  public.workorders_workordercancellationdetail,
  public.workorders_workorderreview,
  public.core_workordersummary,
  public.core_workorderclientinvoicesapproval,
  public.core_workorderdispute,
  public.core_workorderinvoicestrategy,
  public.core_clientinvoice,
  public.core_clientinvoicelineitem,
  public.core_clientinvoicetax,
  public.core_clientmanagedinvoice,
  public.users_organization,
  public.users_organization_tos_agreements,
  public.core_organizationfeatures,
  public.core_organizationsubscription
to cdsync_staging;

drop publication if exists cdsync_staging_pub;

create publication cdsync_staging_pub for table
  public.workorders_workorder,
  public.workorders_workordercancellationdetail,
  public.workorders_workorderreview,
  public.core_workordersummary,
  public.core_workorderclientinvoicesapproval,
  public.core_workorderdispute,
  public.core_workorderinvoicestrategy,
  public.core_clientinvoice,
  public.core_clientinvoicelineitem,
  public.core_clientinvoicetax,
  public.core_clientmanagedinvoice,
  public.users_organization,
  public.users_organization_tos_agreements,
  public.core_organizationfeatures,
  public.core_organizationsubscription;

commit;

select pubname
from pg_publication
where pubname = 'cdsync_staging_pub';

select schemaname, tablename
from pg_publication_tables
where pubname = 'cdsync_staging_pub'
order by schemaname, tablename;

select rolname, rolcanlogin
from pg_roles
where rolname = 'cdsync_staging';

select pg_has_role('cdsync_staging', 'rds_replication', 'member') as has_rds_replication;
