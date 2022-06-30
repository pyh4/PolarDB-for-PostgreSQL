import{_ as r,r as t,o as p,c as i,a as n,b as a,w as e,d as s,e as d}from"./app.71a73901.js";const u={},k=n("h3",{id:"\u901A\u8FC7-docker-\u5FEB\u901F\u4F7F\u7528",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#\u901A\u8FC7-docker-\u5FEB\u901F\u4F7F\u7528","aria-hidden":"true"},"#"),s(" \u901A\u8FC7 Docker \u5FEB\u901F\u4F7F\u7528")],-1),_=s("\u4ECE DockerHub \u4E0A\u62C9\u53D6 PolarDB for PostgreSQL \u7684 "),b={href:"https://hub.docker.com/r/polardb/polardb_pg_local_instance/tags",target:"_blank",rel:"noopener noreferrer"},m=s("\u672C\u5730\u5B58\u50A8\u5B9E\u4F8B\u955C\u50CF"),h=s("\uFF0C\u521B\u5EFA\u3001\u8FD0\u884C\u5E76\u8FDB\u5165\u5BB9\u5668\uFF0C\u7136\u540E\u76F4\u63A5\u4F7F\u7528 PolarDB \u5B9E\u4F8B\uFF1A"),v=n("div",{class:"language-bash ext-sh"},[n("pre",{class:"language-bash"},[n("code",null,[n("span",{class:"token comment"},"# \u62C9\u53D6\u5355\u8282\u70B9 PolarDB \u955C\u50CF"),s(`
`),n("span",{class:"token function"},"docker"),s(` pull polardb/polardb_pg_local_instance:single
`),n("span",{class:"token comment"},"# \u521B\u5EFA\u8FD0\u884C\u5E76\u8FDB\u5165\u5BB9\u5668"),s(`
`),n("span",{class:"token function"},"docker"),s(" run -it --cap-add"),n("span",{class:"token operator"},"="),s("SYS_PTRACE --privileged"),n("span",{class:"token operator"},"="),s("true --name polardb_pg_single polardb/polardb_pg_local_instance:single "),n("span",{class:"token function"},"bash"),s(`
`),n("span",{class:"token comment"},"# \u6D4B\u8BD5\u5B9E\u4F8B\u53EF\u7528\u6027"),s(`
psql -h `),n("span",{class:"token number"},"127.0"),s(".0.1 -c "),n("span",{class:"token string"},"'select version();'"),s(`
            version
--------------------------------
 PostgreSQL `),n("span",{class:"token number"},"11.9"),s(),n("span",{class:"token punctuation"},"("),s("POLARDB "),n("span",{class:"token number"},"11.9"),n("span",{class:"token punctuation"},")"),s(`
`),n("span",{class:"token punctuation"},"("),n("span",{class:"token number"},"1"),s(" row"),n("span",{class:"token punctuation"},")"),s(`
`)])])],-1),g=n("div",{class:"language-bash ext-sh"},[n("pre",{class:"language-bash"},[n("code",null,[n("span",{class:"token comment"},"# \u62C9\u53D6\u591A\u8282\u70B9 PolarDB \u955C\u50CF"),s(`
`),n("span",{class:"token function"},"docker"),s(` pull polardb/polardb_pg_local_instance:withrep
`),n("span",{class:"token comment"},"# \u521B\u5EFA\u8FD0\u884C\u5E76\u8FDB\u5165\u5BB9\u5668"),s(`
`),n("span",{class:"token function"},"docker"),s(" run -it --cap-add"),n("span",{class:"token operator"},"="),s("SYS_PTRACE --privileged"),n("span",{class:"token operator"},"="),s("true --name polardb_pg_withrep polardb/polardb_pg_local_instance:withrep "),n("span",{class:"token function"},"bash"),s(`
`),n("span",{class:"token comment"},"# \u6D4B\u8BD5\u5B9E\u4F8B\u53EF\u7528\u6027"),s(`
psql -h `),n("span",{class:"token number"},"127.0"),s(".0.1 -c "),n("span",{class:"token string"},"'select version();'"),s(`
            version
--------------------------------
 PostgreSQL `),n("span",{class:"token number"},"11.9"),s(),n("span",{class:"token punctuation"},"("),s("POLARDB "),n("span",{class:"token number"},"11.9"),n("span",{class:"token punctuation"},")"),s(`
`),n("span",{class:"token punctuation"},"("),n("span",{class:"token number"},"1"),s(" row"),n("span",{class:"token punctuation"},")"),s(`
`)])])],-1),f=n("div",{class:"language-bash ext-sh"},[n("pre",{class:"language-bash"},[n("code",null,[n("span",{class:"token comment"},"# \u62C9\u53D6 HTAP PolarDB \u955C\u50CF"),s(`
`),n("span",{class:"token function"},"docker"),s(` pull polardb/polardb_pg_local_instance:htap
`),n("span",{class:"token comment"},"# \u521B\u5EFA\u8FD0\u884C\u5E76\u8FDB\u5165\u5BB9\u5668"),s(`
`),n("span",{class:"token function"},"docker"),s(" run -it --cap-add"),n("span",{class:"token operator"},"="),s("SYS_PTRACE --privileged"),n("span",{class:"token operator"},"="),s("true --name polardb_pg_htap polardb/polardb_pg_local_instance:htap "),n("span",{class:"token function"},"bash"),s(`
`),n("span",{class:"token comment"},"# \u6D4B\u8BD5\u5B9E\u4F8B\u53EF\u7528\u6027"),s(`
psql -h `),n("span",{class:"token number"},"127.0"),s(".0.1 -c "),n("span",{class:"token string"},"'select version();'"),s(`
            version
--------------------------------
 PostgreSQL `),n("span",{class:"token number"},"11.9"),s(),n("span",{class:"token punctuation"},"("),s("POLARDB "),n("span",{class:"token number"},"11.9"),n("span",{class:"token punctuation"},")"),s(`
`),n("span",{class:"token punctuation"},"("),n("span",{class:"token number"},"1"),s(" row"),n("span",{class:"token punctuation"},")"),s(`
`)])])],-1),P=n("h3",{id:"\u901A\u8FC7-docker-\u5FEB\u901F\u5F00\u53D1",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#\u901A\u8FC7-docker-\u5FEB\u901F\u5F00\u53D1","aria-hidden":"true"},"#"),s(" \u901A\u8FC7 Docker \u5FEB\u901F\u5F00\u53D1")],-1),B=s("\u4ECE DockerHub \u4E0A\u62C9\u53D6 PolarDB for PostgreSQL \u7684 "),D={href:"https://hub.docker.com/r/polardb/polardb_pg_devel/tags",target:"_blank",rel:"noopener noreferrer"},S=s("\u5F00\u53D1\u955C\u50CF"),x=s("\uFF0C\u521B\u5EFA\u3001\u8FD0\u884C\u5E76\u8FDB\u5165\u5BB9\u5668\uFF1A"),L=d(`<div class="language-bash ext-sh line-numbers-mode"><pre class="language-bash"><code><span class="token comment"># \u62C9\u53D6 PolarDB \u5F00\u53D1\u955C\u50CF</span>
<span class="token function">docker</span> pull polardb/polardb_pg_devel
<span class="token comment"># \u521B\u5EFA\u3001\u8FD0\u884C\u5E76\u8FDB\u5165\u5BB9\u5668</span>
<span class="token function">docker</span> run -it --cap-add<span class="token operator">=</span>SYS_PTRACE --privileged<span class="token operator">=</span>true --name polardb_pg_devel polardb/polardb_pg_devel <span class="token function">bash</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>\u8FDB\u5165\u5BB9\u5668\u540E\uFF0C\u4ECE GitHub \u62C9\u53D6\u6700\u65B0\u7684\u7A33\u5B9A\u4EE3\u7801\uFF0C\u5FEB\u901F\u7F16\u8BD1\u90E8\u7F72\u6700\u7B80\u5355\u7684 PolarDB \u5B9E\u4F8B\u5E76\u8FDB\u884C\u9A8C\u8BC1\uFF1A</p><div class="language-bash ext-sh line-numbers-mode"><pre class="language-bash"><code><span class="token comment"># \u4EE3\u7801\u62C9\u53D6</span>
<span class="token function">git</span> clone -b POLARDB_11_STABLE https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
<span class="token builtin class-name">cd</span> PolarDB-for-PostgreSQL
<span class="token comment"># \u7F16\u8BD1\u90E8\u7F72</span>
./polardb_build.sh
<span class="token comment"># \u9A8C\u8BC1</span>
psql -h <span class="token number">127.0</span>.0.1 -c <span class="token string">&#39;select version();&#39;</span>
            version
--------------------------------
 PostgreSQL <span class="token number">11.9</span> <span class="token punctuation">(</span>POLARDB <span class="token number">11.9</span><span class="token punctuation">)</span>
<span class="token punctuation">(</span><span class="token number">1</span> row<span class="token punctuation">)</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,3);function A(C,w){const l=t("ExternalLinkIcon"),o=t("CodeGroupItem"),c=t("CodeGroup");return p(),i("div",null,[k,n("p",null,[_,n("a",b,[m,a(l)]),h]),a(c,null,{default:e(()=>[a(o,{title:"\u5355\u8282\u70B9\u5B9E\u4F8B"},{default:e(()=>[v]),_:1}),a(o,{title:"\u591A\u8282\u70B9\u5B9E\u4F8B"},{default:e(()=>[g]),_:1}),a(o,{title:"HTAP \u5B9E\u4F8B"},{default:e(()=>[f]),_:1})]),_:1}),P,n("p",null,[B,n("a",D,[S,a(l)]),x]),L])}var E=r(u,[["render",A],["__file","index.html.vue"]]);export{E as default};
