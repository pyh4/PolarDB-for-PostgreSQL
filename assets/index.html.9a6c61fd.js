import{_ as r,r as o,o as p,c as i,a as n,b as s,w as a,d as e,e as d}from"./app.71a73901.js";const u={},k=n("h3",{id:"use-with-docker",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#use-with-docker","aria-hidden":"true"},"#"),e(" Use with Docker")],-1),m=e("Pull the "),h={href:"https://hub.docker.com/r/polardb/polardb_pg_local_instance/tags",target:"_blank",rel:"noopener noreferrer"},b=e("instance image"),_=e(" of PolarDB for PostgreSQL based on local storage. Create, run and enter the container, and use PolarDB instance directly:"),g=n("div",{class:"language-bash ext-sh"},[n("pre",{class:"language-bash"},[n("code",null,[n("span",{class:"token comment"},"# pull the instance image from DockerHub"),e(`
`),n("span",{class:"token function"},"docker"),e(` pull polardb/polardb_pg_local_instance:single
`),n("span",{class:"token comment"},"# create, run and enter the container"),e(`
`),n("span",{class:"token function"},"docker"),e(" run -it --cap-add"),n("span",{class:"token operator"},"="),e("SYS_PTRACE --privileged"),n("span",{class:"token operator"},"="),e("true --name polardb_pg_single polardb/polardb_pg_local_instance:single "),n("span",{class:"token function"},"bash"),e(`
`),n("span",{class:"token comment"},"# check"),e(`
psql -h `),n("span",{class:"token number"},"127.0"),e(".0.1 -c "),n("span",{class:"token string"},"'select version();'"),e(`
            version
--------------------------------
 PostgreSQL `),n("span",{class:"token number"},"11.9"),e(),n("span",{class:"token punctuation"},"("),e("POLARDB "),n("span",{class:"token number"},"11.9"),n("span",{class:"token punctuation"},")"),e(`
`),n("span",{class:"token punctuation"},"("),n("span",{class:"token number"},"1"),e(" row"),n("span",{class:"token punctuation"},")"),e(`
`)])])],-1),v=n("div",{class:"language-bash ext-sh"},[n("pre",{class:"language-bash"},[n("code",null,[n("span",{class:"token comment"},"# pull the instance image from DockerHub"),e(`
`),n("span",{class:"token function"},"docker"),e(` pull polardb/polardb_pg_local_instance:withrep
`),n("span",{class:"token comment"},"# create, run and enter the container"),e(`
`),n("span",{class:"token function"},"docker"),e(" run -it --cap-add"),n("span",{class:"token operator"},"="),e("SYS_PTRACE --privileged"),n("span",{class:"token operator"},"="),e("true --name polardb_pg_withrep polardb/polardb_pg_local_instance:withrep "),n("span",{class:"token function"},"bash"),e(`
`),n("span",{class:"token comment"},"# check"),e(`
psql -h `),n("span",{class:"token number"},"127.0"),e(".0.1 -c "),n("span",{class:"token string"},"'select version();'"),e(`
            version
--------------------------------
 PostgreSQL `),n("span",{class:"token number"},"11.9"),e(),n("span",{class:"token punctuation"},"("),e("POLARDB "),n("span",{class:"token number"},"11.9"),n("span",{class:"token punctuation"},")"),e(`
`),n("span",{class:"token punctuation"},"("),n("span",{class:"token number"},"1"),e(" row"),n("span",{class:"token punctuation"},")"),e(`
`)])])],-1),f=n("div",{class:"language-bash ext-sh"},[n("pre",{class:"language-bash"},[n("code",null,[n("span",{class:"token comment"},"# pull the instance image from DockerHub"),e(`
`),n("span",{class:"token function"},"docker"),e(` pull polardb/polardb_pg_local_instance:htap
`),n("span",{class:"token comment"},"# create, run and enter the container"),e(`
`),n("span",{class:"token function"},"docker"),e(" run -it --cap-add"),n("span",{class:"token operator"},"="),e("SYS_PTRACE --privileged"),n("span",{class:"token operator"},"="),e("true --name polardb_pg_htap polardb/polardb_pg_local_instance:htap "),n("span",{class:"token function"},"bash"),e(`
`),n("span",{class:"token comment"},"# check"),e(`
psql -h `),n("span",{class:"token number"},"127.0"),e(".0.1 -c "),n("span",{class:"token string"},"'select version();'"),e(`
            version
--------------------------------
 PostgreSQL `),n("span",{class:"token number"},"11.9"),e(),n("span",{class:"token punctuation"},"("),e("POLARDB "),n("span",{class:"token number"},"11.9"),n("span",{class:"token punctuation"},")"),e(`
`),n("span",{class:"token punctuation"},"("),n("span",{class:"token number"},"1"),e(" row"),n("span",{class:"token punctuation"},")"),e(`
`)])])],-1),P=n("h3",{id:"develop-with-docker",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#develop-with-docker","aria-hidden":"true"},"#"),e(" Develop with Docker")],-1),D=e("Pull the "),S={href:"https://hub.docker.com/r/polardb/polardb_pg_devel/tags",target:"_blank",rel:"noopener noreferrer"},B=e("development image"),x=e(" of PolarDB for PostgreSQL from DockerHub. Create, run and enter the container:"),L=d(`<div class="language-bash ext-sh line-numbers-mode"><pre class="language-bash"><code><span class="token comment"># pull the development image of PolarDB</span>
<span class="token function">docker</span> pull polardb/polardb_pg_devel
<span class="token comment"># create, run and enter the container</span>
<span class="token function">docker</span> run -it --cap-add<span class="token operator">=</span>SYS_PTRACE --privileged<span class="token operator">=</span>true --name polardb_pg_devel polardb/polardb_pg_devel <span class="token function">bash</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>After entering the container, clone the latest stable code from GitHub, build and deploy the simplest PolarDB instance and check:</p><div class="language-bash ext-sh line-numbers-mode"><pre class="language-bash"><code><span class="token comment"># code fetching</span>
<span class="token function">git</span> clone -b POLARDB_11_STABLE https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
<span class="token builtin class-name">cd</span> PolarDB-for-PostgreSQL
<span class="token comment"># build and deploy</span>
./polardb_build.sh
<span class="token comment"># check</span>
psql -h <span class="token number">127.0</span>.0.1 -c <span class="token string">&#39;select version();&#39;</span>
            version
--------------------------------
 PostgreSQL <span class="token number">11.9</span> <span class="token punctuation">(</span>POLARDB <span class="token number">11.9</span><span class="token punctuation">)</span>
<span class="token punctuation">(</span><span class="token number">1</span> row<span class="token punctuation">)</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,3);function w(A,C){const c=o("ExternalLinkIcon"),t=o("CodeGroupItem"),l=o("CodeGroup");return p(),i("div",null,[k,n("p",null,[m,n("a",h,[b,s(c)]),_]),s(l,null,{default:a(()=>[s(t,{title:"Single-Node"},{default:a(()=>[g]),_:1}),s(t,{title:"Multi-Node"},{default:a(()=>[v]),_:1}),s(t,{title:"HTAP"},{default:a(()=>[f]),_:1})]),_:1}),P,n("p",null,[D,n("a",S,[B,s(c)]),x]),L])}var E=r(u,[["render",w],["__file","index.html.vue"]]);export{E as default};
