#define _GNU_SOURCE
#include <sched.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <pthread.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <sys/ipc.h>  
#include <sys/shm.h>  
#include <errno.h>
#include <signal.h>
#include<sys/wait.h>
#include <sys/stat.h> /* For mode constants */
#include <fcntl.h> /* For O_* constants */



#include <atomic_user.h>
#include <lf_order.h>

void pk_send(u64 cmd)
{
    return;
}


static inline void *alloc_page()
{
    void *p;
    p = malloc(4096);
    if(p != 0)
    {
        memset(p, 0, 4096);
    }
    return p;
}

static inline void free_page(void *page)
{
    free(page);
    return;
}
//static
int __lxd_alloc(u64 *lxud, orderq_h_t *oq)
{
    u64 *new_pg, *pg;
    new_pg = (u64 *)alloc_page();
    if(new_pg == NULL)
    {
        lforder_debug("OOM\n");
        return LO_OOM;
    }
    pg = (u64 *)atomic64_cmpxchg((atomic64_t *)lxud, 0, (long)new_pg);
    if(pg != 0)
    {
        free_page(new_pg);
    }
    else
    {
        atomic_add(1, (atomic_t *)&oq->pg_num);
    }
    return LO_OK;
}

static inline u64 *l0d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l0_index(oid);
}
static inline u64 *l1d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l1_index(oid);
}
static inline u64 *l2d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l2_index(oid);
}
static inline u64 *l3d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l3_index(oid);
}
static inline u64 *l4d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l4_index(oid);
}
static inline u64 *l5d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l5_index(oid);
}
static inline u64 *l6d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l6_index(oid);
}
static inline u64 *l7d_offset(u64 *lxud, u64 oid)
{
	return (u64 *)(*lxud) + l7_index(oid);
}

static inline u64 *l0d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l0d_offset(lxud, oid);
}
static inline u64 *l1d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l1d_offset(lxud, oid);
}
static inline u64 *l2d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l2d_offset(lxud, oid);
}
static inline u64 *l3d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l3d_offset(lxud, oid);
}
static inline u64 *l4d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l4d_offset(lxud, oid);
}
static inline u64 *l5d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l5d_offset(lxud, oid);
}
static inline u64 *l6d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l6d_offset(lxud, oid);
}
static inline u64 *l7d_alloc(u64 *lxud, u64 oid, orderq_h_t *oq)
{
	return (unlikely(lxd_none(*lxud)) && __lxd_alloc(lxud, oq))?
		NULL: l7d_offset(lxud, oid);
}



orderq_h_t *lfo_q_init(int thread_num)
{
    orderq_h_t *oq = NULL;
    int i;

    oq = (orderq_h_t *)malloc(sizeof(orderq_h_t) + sizeof(u64)*thread_num + sizeof(u64 *)*thread_num);
    if(oq == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
    memset((void *)oq, 0, sizeof(orderq_h_t));

    oq->thd_num = thread_num;

    oq->l0_fd = (u64 *)alloc_page();
    if(oq->l0_fd == NULL)
    {
        free(oq);
        lforder_debug("OOM\n");
        return NULL;
    }
    memset((void *)oq->l0_fd, 0, 4096);
    oq->pg_num = 1;
    oq->l0_fd[0] = ORDER_TOKEN;

    oq->local_oid = (u64 *)((u64)oq + sizeof(orderq_h_t));
    memset((void *)oq->local_oid, 0, sizeof(u64)*thread_num);

    oq->local_l0_pg = (u64 **)((u64)oq->local_oid + sizeof(u64)*thread_num);
    for(i=0; i<thread_num; i++)
    {
        oq->local_l0_pg[i] = oq->l0_fd;
    }
    return oq;
}

//static
u64 *get_l0_pg(orderq_h_t *oq, int thread, u64 oid)
{
    u64 *lv0d,*lv1d,*lv2d,*lv3d,*lv4d,*lv5d,*lv6d, *lv7d;
    u64 last_oid;
    last_oid = oq->local_oid[thread];
    oq->local_oid[thread] = oid;
    if(oids_in_same_page(oid, last_oid))
    {
        oq->local_oid[thread] = oid;//可以不赋值，等到换页时再赋值
        return oq->local_l0_pg[thread];
    }

//    if(oid < l0_PTRS_PER_PG)
    if(oid < l1_PTRS_PER_PG)
    {
        lv2d = (u64 *)&oq->l1_fd;
        goto get_lv1d;
    }
    else if(oid < l2_PTRS_PER_PG)
    {
        lv3d = (u64 *)&oq->l2_fd;
        goto get_lv2d;
    }        
    else if(oid < l3_PTRS_PER_PG)
    {
        lv4d = (u64 *)&oq->l3_fd;
        goto get_lv3d;
    }        
    else if(oid < l4_PTRS_PER_PG)
    {
        lv5d = (u64 *)&oq->l4_fd;
        goto get_lv4d;
    }        
    else if(oid < l5_PTRS_PER_PG)
    {
        lv6d = (u64 *)&oq->l5_fd;
        goto get_lv5d;
    }
    else if(oid < l6_PTRS_PER_PG) 
    {
        lv7d = (u64 *)&oq->l6_fd;
        goto get_lv6d;
    }
    else
    {
        goto get_lv7d;
    }

get_lv7d:    
    lv7d = l7d_alloc((u64 *)&oq->l7_fd, oid, oq);
    if(lv7d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv6d:        
    lv6d = l6d_alloc(lv7d, oid, oq);
    if(lv6d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv5d:
    lv5d = l5d_alloc(lv6d, oid, oq);
    if(lv5d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv4d:
    lv4d = l4d_alloc(lv5d, oid, oq);
    if(lv4d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv3d:        
    lv3d = l3d_alloc(lv4d, oid, oq);
    if(lv3d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv2d:
    lv2d = l2d_alloc(lv3d, oid, oq);
    if(lv2d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }
get_lv1d:
    lv1d = l1d_alloc(lv2d, oid, oq);
    if(lv1d == NULL)
    {
        lforder_debug("OOM\n");
        return NULL;
    }

    if(unlikely(lxd_none(*lv1d)) && unlikely(__lxd_alloc(lv1d, oq)))
    {
        lforder_debug("OOM\n");
        return NULL;
    }   
    oq->local_l0_pg[thread] = *lv1d;
    oq->newest_pg = oid >> 9;
    return (u64 *)*lv1d;
}

void _free_finished_pg(orderq_h_t *oq, void *pg)
{
    u64 *lv1d,*lv2d,*lv3d,*lv4d,*lv5d,*lv6d, *lv7d, *ret;
    u64 oid;
    if(oq->pg_num > oq->pg_max)
    {
        free_page((void *)pg);
        atomic_sub(1, (atomic_t *)&oq->pg_num);
        return;
    }
    oid = (oq->newest_pg + 1)<<9;
    
    if(oid < l1_PTRS_PER_PG)
    {
        lv1d = l1d_alloc((u64 *)&oq->l1_fd, oid, oq);
        if(lv1d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
    }
    else if(oid < l2_PTRS_PER_PG)
    {
        lv2d = l2d_alloc((u64 *)&oq->l2_fd, oid, oq);
        if(lv2d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }    
        lv1d = l1d_alloc(lv2d, oid, oq);
        if(lv1d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
    }        
    else if(oid < l3_PTRS_PER_PG)
    {
        lv3d = l3d_alloc((u64 *)&oq->l3_fd, oid, oq);
        if(lv3d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
        lv2d = l2d_alloc(lv3d, oid, oq);
        if(lv2d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }        
        lv1d = l1d_alloc(lv2d, oid, oq);
        if(lv1d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
    }        
    else if(oid < l4_PTRS_PER_PG)
    {
        lv4d = l4d_alloc((u64 *)&oq->l4_fd, oid, oq);
        if(lv4d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
        lv3d = l3d_alloc(lv4d, oid, oq);
        if(lv3d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }        
        
        lv2d = l2d_alloc(lv3d, oid, oq);
        if(lv2d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }        
        lv1d = l1d_alloc(lv2d, oid, oq);
        if(lv1d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
    }        
    else if(oid < l5_PTRS_PER_PG)
    {
        lv5d = l5d_alloc((u64 *)&oq->l5_fd, oid, oq);
        if(lv5d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
        lv4d = l4d_alloc(lv5d, oid, oq);
        if(lv4d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }                
        lv3d = l3d_alloc(lv4d, oid, oq);
        if(lv3d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }        
        lv2d = l2d_alloc(lv3d, oid, oq);
        if(lv2d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }        
        lv1d = l1d_alloc(lv2d, oid, oq);
        if(lv1d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
    }
    else if(oid < l6_PTRS_PER_PG) 
    {
        lv6d = l6d_alloc((u64 *)&oq->l6_fd, oid, oq);
        if(lv6d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
        lv5d = l5d_alloc(lv6d, oid, oq);
        if(lv5d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }                       
        lv4d = l4d_alloc(lv5d, oid, oq);
        if(lv4d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }                
        lv3d = l3d_alloc(lv4d, oid, oq);
        if(lv3d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }        
        lv2d = l2d_alloc(lv3d, oid, oq);
        if(lv2d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }        
        lv1d = l1d_alloc(lv2d, oid, oq);
        if(lv1d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
    }
    else
    {
        lv7d = l7d_alloc((u64 *)&oq->l7_fd, oid, oq);
        if(lv7d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
        lv6d = l6d_alloc(lv7d, oid, oq);
        if(lv6d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }                               
        lv5d = l5d_alloc(lv6d, oid, oq);
        if(lv5d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }                       
        lv4d = l4d_alloc(lv5d, oid, oq);
        if(lv4d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }                
        lv3d = l3d_alloc(lv4d, oid, oq);
        if(lv3d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }        
        lv2d = l2d_alloc(lv3d, oid, oq);
        if(lv2d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }        
        lv1d = l1d_alloc(lv2d, oid, oq);
        if(lv1d == NULL)
        {
            lforder_debug("OOM\n");
            return NULL;
        }
    }
    if(lxd_none(*lv1d))
    {
        ret= (u64 *)atomic64_cmpxchg((atomic64_t *)lv1d, 0, (long)pg);
        if(ret != 0)
        {
            free_page(pg);
            atomic_sub(1, (atomic_t *)&oq->pg_num);
        }
    }
    return;
}


/*oid 低9bit肯定为0*/
void lfo_free_finished_pgs(orderq_h_t *oq, u64 oid)
{
    u64 *lvxd,*lv1d,*lv2d,*lv3d,*lv4d,*lv5d,*lv6d, *lv7d, *pg,*pg2free;
    u64 last_oid, index;
    int level;

    if(oid < l1_PTRS_PER_PG)
    {
        level = 0;
        index = l1_index(oid) - 1;
        if(index == 0)
        {
            pg = oq->l0_fd;
            oq->l0_fd = 0;
        }
        else
        {
            lvxd = oq->l1_fd + index;
            pg = *lvxd;
            *lvxd = 0;
        }
    }
    else if(oid < l2_PTRS_PER_PG)
    {
        level = 1;
        index = l2_index(oid) - 1;
        if(index == 0)
        {
            pg = oq->l1_fd;
            oq->l1_fd = 0;
        }
        else
        {
            lvxd = oq->l2_fd + index;
            pg = *lvxd;
            *lvxd = 0;
        }
    }
    else if(oid < l3_PTRS_PER_PG)
    {
        level = 2;
        index = l3_index(oid) - 1;
        if(index == 0)
        {
            pg = oq->l2_fd;
            oq->l2_fd = 0;
        }
        else
        {
            lvxd = oq->l3_fd + index;
            pg = *lvxd;
            *lvxd = 0;
        }
    }        
    else if(oid < l4_PTRS_PER_PG)
    {
        level = 3;
        index = l4_index(oid) - 1;
        if(index == 0)
        {
            pg = oq->l3_fd;
            oq->l3_fd = 0;
        }
        else
        {
            lvxd = oq->l4_fd + index;
            pg = *lvxd;
            *lvxd = 0;
        }
    }        
    else if(oid < l5_PTRS_PER_PG)
    {
        level = 4;
        index = l5_index(oid) - 1;
        if(index == 0)
        {
            pg = oq->l4_fd;
            oq->l4_fd = 0;
        }
        else
        {
            lvxd = oq->l5_fd + index;
            pg = *lvxd;
            *lvxd = 0;
        }
    }
    else if(oid < l6_PTRS_PER_PG) 
    {
        level = 5;
        index = l6_index(oid) - 1;
        if(index == 0)
        {
            pg = oq->l5_fd;
            oq->l5_fd = 0;
        }
        else
        {
            lvxd = oq->l6_fd + index;
            pg = *lvxd;
            *lvxd = 0;
        }
    }
    else
    {
        level = 6;
        index = l7_index(oid) - 1;
        if(index == 0)
        {
            pg = oq->l6_fd;
            oq->l6_fd = 0;
        }
        else
        {
            lvxd = oq->l7_fd + index;
            pg = *lvxd;
            *lvxd = 0;
        }
    }
    
    while(level > 0)
    {
        if(oid &((1ul<<(9*(level+1)))-1) == 0)
        {
            break;
        }
        index = (oid >> 9*level)&(PTRS_PER_LEVEL - 1);
        lvxd = pg + index-1;
        pg = *lvxd;
        *lvxd = 0;
        level--;
    }

    while(level > 0)
    {
        lvxd = pg + PTRS_PER_LEVEL-1;
        pg2free = pg;
        pg = *lvxd;
        *lvxd = 0;
        _free_finished_pg(oq, pg2free);
        level--;
    }
     _free_finished_pg(oq, pg);
    return;
}


int lfo_send(orderq_h_t *oq, int thread, u64 cmd, u64 oid)
{
    u64 *cur_pg, *entry;
    u64 val;
    int idx;

    cur_pg = get_l0_pg(oq, thread, oid);
    if(cur_pg == NULL)
    {
        lforder_debug("OOM\n");
        return LO_OOM;
    }
    idx = l0_index(oid);
    entry = cur_pg + idx;
    
    if(*entry == ORDER_TOKEN)
    {
        goto snd_pkt;
    }
    else
    {
        val = atomic64_xchg((atomic64_t *)entry, cmd);
        if(val == ORDER_TOKEN)
        {
            goto snd_pkt;
        }
        else
        {
            return LO_OK;
        }
    }

snd_pkt:    
    pk_send(cmd);
    *entry = 0;
    entry++;
    oid++;
    idx++;
    
    if(idx == PTRS_PER_LEVEL)
    {
        //1:free pages 

        //2:get new position
        cur_pg = get_l0_pg(oq, thread, oid);
        if(cur_pg == NULL)
        {
            lforder_debug("OOM\n");
            return LO_OOM;
        }
        idx = l0_index(oid);
        entry = cur_pg + idx;
    }        

    if(*entry != 0)
    {
        cmd = *entry;
        goto snd_pkt;
    }
    else
    {
        val = atomic64_xchg((atomic64_t *)entry, ORDER_TOKEN);
        if(val != 0)
        {
            cmd = val;
            goto snd_pkt;
        }
        else
        {
            return LO_OK;
        }
    }


    
}


