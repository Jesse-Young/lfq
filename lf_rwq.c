
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
#include <lf_rwq.h>
#include <lf_order.h>

#define test_mb

u32 mydebug[5][65536];


void lfrwq_pre_alloc(lfrwq_t* qh);


u64 lfrwq_deq(lfrwq_t* qh, void **ppdata)
{
    u64 idx;
    volatile u64 data;
    
//    q = qh->q;
    idx = atomic64_add_return(1,(atomic64_t *)&qh->r_idx) - 1;
    idx = idx&(qh->len - 1);
    do
    {
        data = qh->q[idx];    
    }while(data == 0);

//    data = atomic64_xchg((atomic64_t *)&q[idx], 0);
    qh->q[idx] = 0;
    *ppdata = (void *)data;
    atomic64_add(1, (atomic64_t *)&qh->dbg_r_total);
    return (idx >> qh->blk_pow);
}

int lfrwq_inq(lfrwq_t* qh, void *data)
{
    u64 idx, laps;
    volatile u64 rd_cnt;
    u32 blk_idx;

    idx = atomic64_add_return(1,(atomic64_t *)&qh->w_idx) - 1;
    laps = idx >> qh->q_pow;
    idx = idx&(qh->len - 1);
    blk_idx = idx >> qh->blk_pow;
    
    do
    {
        rd_cnt = qh->r_cnt[blk_idx];
    }while((rd_cnt >> qh->blk_pow) < laps);
    
    if(qh->q[idx] != 0)
    {
        lfrwq_debug("inq overlap find");
        assert(0);
    }
    qh->q[idx] = (u64)data;

    if((idx&(qh->blk_len - 1)) == 0)
    {
        lfrwq_pre_alloc(qh);
    }
        
    return 0;
}


void lfrwq_add_rcnt(lfrwq_t* qh, u32 total, u32 cnt_idx)
{
    atomic64_add(total,(atomic64_t *)&qh->r_cnt[cnt_idx]);
    return;
}


int lfrwq_get_rpermit(lfrwq_t* qh)
{
    int permit, left, suggest;
try_again:    
    suggest = qh->r_pmt_sgest;
    left = atomic_sub_return(suggest,(atomic_t *)&qh->r_permit);
    if(left + suggest <= 0)
    {
        if(atomic_add_return(suggest, (atomic_t *)&qh->r_permit)>0)
            goto try_again;
        return 0;
    }
    if(left < 0)
    {
        atomic_sub(left, (atomic_t *)&qh->r_permit);
        permit = left + suggest;
        return permit;
    }
    return suggest;
}

void lfrwq_pre_alloc(lfrwq_t* qh)
{
    int alloc;
    
    alloc = qh->blk_len;
    atomic_add((int)alloc, (atomic_t *)&qh->r_permit);
    atomic64_add(alloc, (atomic64_t *)&qh->dbg_p_total);        
    return;
}

lfrwq_t* lfrwq_init(u32 q_len, u32 blk_len, u32 readers)
{
    lfrwq_t *qh;
    u32 quo, blk_cnt, pow1, pow2, total_len;
    int fd;

    quo = q_len;
    pow1 = pow2 = 0;
    while(quo > 1)
    {
        if(quo%2 != 0)
        {
            lfrwq_debug("input err:q_len\n");
            goto init_err;
        }
        quo = quo/2;
        pow1++;
    }

    quo = blk_len;
    while(quo > 1)
    {
        if(quo%2 != 0)
        {
            lfrwq_debug("input err:blk_len\n");
            goto init_err;
        }
        quo = quo/2;
        pow2++;
    }

    blk_cnt = q_len/blk_len;
    total_len = sizeof(u64)*q_len + sizeof(lfrwq_t)+sizeof(u64)*(blk_cnt);

    if( (fd = shm_open("region", O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)) < 0 )
    { 
        lfrwq_debug("Create Share Memory Error:%s/n/a", strerror(errno));  
        goto init_err;  
    }
//    total_len = (1 + (total_len-1)/4096)*4096;
//    (void)ftruncate(fd, total_len);
    if( ftruncate(fd, total_len) != 0 )
    { 
        lfrwq_debug("ftruncate Error:%s/n/a", strerror(errno));  
        goto init_err;  
    }

    qh = mmap(NULL, total_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    
    memset((void *)qh, 0, total_len);
    qh->r_cnt = (u64 *)((long)qh + sizeof(u64)*q_len + sizeof(lfrwq_t));
    #if 0
    if(qh->r_cnt == NULL)
        goto free_qh;
    #endif
    
	qh->r_idx = 0;
    qh->w_idx = 0;
    qh->len = q_len;
    qh->q_pow = pow1;
    qh->blk_len = blk_len;
    qh->blk_pow = pow2;
    qh->readers = readers;
    qh->blk_cnt = blk_cnt;
    qh->r_permit = 0;
    qh->r_pmt_sgest = blk_len/qh->readers;
    qh->dbg_r_total = 0;
    qh->dbg_p_total = 0;
    qh->dbg_get_pmt_total = 0;
    #if 0
    memset((void *)qh->q, 0, q_len*sizeof(u64));
    memset((void *)qh->r_cnt, 0, blk_cnt*sizeof(u64));
    #endif
    return qh;
#if 0    
free_qh:
    free(qh);
#endif
init_err:    
    return NULL;
}

lfrwq_t *gqh;

void *writefn(void *arg)
{
    cpu_set_t mask;
    int i, j;
//    int (*inq)(lfrwq_t* , void*);
    
    i = (long)arg;
    if(i<8)
        j=0;
    else
        j=1;
    
    CPU_ZERO(&mask); 
    CPU_SET(j,&mask);

    if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
    {
        printf("warning: could not set CPU affinity, continuing...\n");
    }
    
    printf("writefn%d,start\n",i);
#if 0
    if(lfrwq_get_token(gqh) == 1)
    {
        inq = lfrwq_inq_m;
    }
    else
    {
        inq = lfrwq_inq;
    }
#endif
    for(i=0;i<1000000000;i++)
    {
        if(0 != lfrwq_inq(gqh, (void *)(long)(i+1)))
        {
            lfrwq_debug("inq fail\n");
            break;
        }
#if 0    
        if(0 != inq(gqh, (void *)(long)(i+1)))
        {
            lfrwq_debug("inq fail\n");
            break;
        }
#endif
    }
    
    while(1)
    {
        sleep(1);
    }
    return 0;    
}

void *readfn(void *arg)
{
    cpu_set_t mask;
    int i, j;
    u32 local_pmt, blk, tmp_blk, cnt;
    u64 *pdata;

    i = (long)arg;

    if(i<24)
        j=2;
    else
        j=3;
   
    CPU_ZERO(&mask); 
    CPU_SET(j,&mask);

    if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
    {
        printf("warning: could not set CPU affinity, continuing...\n");
    }
    
    printf("readfn%d,start\n",i);

    cnt = 0;
    while(1)
    {
        local_pmt = lfrwq_get_rpermit(gqh);
        atomic64_add(local_pmt,(atomic64_t *)&gqh->dbg_get_pmt_total);
        if(local_pmt > 0)
        {
            blk = lfrwq_deq(gqh, (void **)&pdata);
            local_pmt--;
            cnt++;
        }
        while(local_pmt > 0)
        {
            tmp_blk = lfrwq_deq(gqh, (void **)&pdata);            
            if(blk != tmp_blk)
            {
                lfrwq_add_rcnt(gqh, cnt, blk);
 #if 0               
                mydebug[i-2][j] = blk;
                mydebug[i-2][j+1] = cnt;
                j=(j+2)%20480;
#endif
                cnt = 0;
                blk = tmp_blk;
            }
            cnt++;
            local_pmt--;
        }
        if(cnt != 0)
        {
            lfrwq_add_rcnt(gqh, cnt, blk);
#if 0            
            mydebug[i-2][j] = blk;
            mydebug[i-2][j+1] = cnt;
            j=(j+2)%20480;
#endif
            cnt = 0;
        }
        usleep(1);
    }
    return 0;    
}

#if 0
int main()
{
    long num;
    int err;
    pthread_t ntid;
    cpu_set_t mask;

    gqh = lfrwq_init(65536, 1024, 16);
    if(gqh == NULL)
        lfrwq_debug("create q return null\n");

    CPU_ZERO(&mask); 
    memset(mydebug, 0, sizeof(mydebug));
#if 1
    for(num=0; num <16; num++)
    {
        err = pthread_create(&ntid, NULL, writefn, (void *)num);
        if (err != 0)
            printf("can't create thread: %s\n", strerror(err));
    }
    for(; num <32; num++)
    {
        err = pthread_create(&ntid, NULL, readfn, (void *)num);
        if (err != 0)
            printf("can't create thread: %s\n", strerror(err));
    }    
#endif

    while(1)
    {
        sleep(1);
    }
    
    return 0;
}
#else
static void sig_child(int signo);
volatile u64 testq[65536];

void *write_s(void *arg)
{
    cpu_set_t mask;
    int i, j;
//    int (*inq)(lfrwq_t* , void*);
    unsigned int start, end;
    i = (long)arg;
    if(i<8)
        j=0;
    else
        j=1;
    j = i;
    CPU_ZERO(&mask); 
    CPU_SET(i,&mask);

    if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
    {
        printf("warning: could not set CPU affinity, continuing...\n");
    }
    
    printf("writefn%d,start\n",i);
#if 0
    if(lfrwq_get_token(gqh) == 1)
    {
        inq = lfrwq_inq_m;
    }
    else
    {
        inq = lfrwq_inq;
    }
#endif
    rdtscl(start);

    for(i=i-1;i<65536;i=i+2)
    {
        #ifdef test_mb
        testq[i] = 0xdeadbeefdeadbeeful;
        smp_mb();
        #else
        atomic64_xchg((atomic64_t *)&testq[i],0xdeadbeefdeadbeeful);
        #endif
    
#if 0    
        if(0 != inq(gqh, (void *)(long)(i+1)))
        {
            lfrwq_debug("inq fail\n");
            break;
        }
#endif
    }
    rdtscl(end);
    end = end - start;
    lfrwq_debug("write%d cycle: %d\n", j,end);
    while(1)
    {
        sleep(1);
    }
    return 0;    
}

void *read_s(void *arg)
{
    cpu_set_t mask;
    int i, j;
    u64 data;
    unsigned int start, end;

    i = (long)arg;

    if(i<24)
        j=2;
    else
        j=3;

    i = j;
    CPU_ZERO(&mask); 
    CPU_SET(i,&mask);

    if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
    {
        printf("warning: could not set CPU affinity, continuing...\n");
    }
    sleep(1);
    printf("readfn%d,start\n",i);
    
    rdtscl(start);
    for(i=0;i<65536;i++)
    {
        #ifdef test_mb
//        while (testq[i] == 0)
//            continue;
        data = testq[i];
        testq[i] = 0;
        #else
        data = atomic64_xchg((atomic64_t *)&testq[i],0);
        #endif
    }    
    rdtscl(end);
    end = end - start;
    lfrwq_debug("read%d cycle: %d\n", j, end);
    
    while(1)
    {
        sleep(1);
    }
    data = data;
    return 0;    
}


int main_test(int argc, char **argv)  
{
    long num;
    int err;
    pthread_t ntid;
    cpu_set_t mask;

    CPU_ZERO(&mask); 
    memset(&testq, 0, sizeof(testq));
    
#if 1
    for(num=1; num <3; num++)
    {
        err = pthread_create(&ntid, NULL, write_s, (void *)num);
        if (err != 0)
            printf("can't create thread: %s\n", strerror(err));
    }
    for(; num <4; num++)
    {
        err = pthread_create(&ntid, NULL, read_s, (void *)num);
        if (err != 0)
            printf("can't create thread: %s\n", strerror(err));
    }    
#endif

    while(1)
    {
        sleep(1);
    }
    
    return 0;
}


int main1(int argc, char **argv)  
{  
    long num;
    pid_t pid;
    signal(SIGCHLD,sig_child);
    
    gqh = lfrwq_init(65536, 1024, 2);
    if(gqh == NULL)
        lfrwq_debug("create q return null\n");

    for(num=0; num < 2; num++)
    {
        pid = fork();
        if(pid < 0)
        {
            printf("fork fail\n");
        }
        else if(pid == 0) 
        {
            sleep(2);
            writefn((void *)num);
        }  
        else
        {  
            ;
        }

    }

    for(; num < 4; num++)
    {
        pid = fork();
        if(pid < 0)
        {
            printf("fork fail\n");
        }
        else if(pid == 0) 
        {
            readfn((void *)num);
        }  
        else
        {  
            ;
        }

    }
    while(1)
    {
        sleep(1);
    }
    return 0;
}  

static void sig_child(int signo)
{
    pid_t pid;
    int stat;
    //处理僵尸进程
    while ((pid = waitpid(-1, &stat, WNOHANG)) >0)
        printf("child %d terminated.\n", pid);
}



#endif


