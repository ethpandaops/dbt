#! /bin/bash

# custom css
cp custom.css target
sed -i 's/<head>/<head>\n<link rel=\"stylesheet\" href=\"\/custom.css\" \/>/' target/index.html
cp logo.svg target

# custom title
sed -i 's|<title>.*</title>|<title>dbt Docs - ethPandaOps</title>|' target/index.html
sed -i 's|<meta property="og:site_name" content=".*" />|<meta property="og:site_name" content="dbt Docs - ethPandaOps" />|' target/index.html
sed -i 's|<meta property="og:title" content=".*" />|<meta property="og:title" content="dbt Docs - ethPandaOps" />|' target/index.html
sed -i 's|<meta name="twitter:title" content=".*"/>|<meta name="twitter:title" content="dbt Docs - ethPandaOps"/>|' target/index.html

# custom description
sed -i 's|<meta name="description" content=".*" />|<meta name="description" content="dbt Docs - ethPandaOps" />|' target/index.html
sed -i 's|<meta property="og:description" content=".*" />|<meta property="og:description" content="documentation for dbt - ethPandaOps" />|' target/index.html
sed -i 's|<meta name="twitter:description" content=".*"/>|<meta name="twitter:description" content="documentation for dbt - ethPandaOps"/>|' target/index.html
