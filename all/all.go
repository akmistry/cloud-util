package all

import (
	_ "github.com/akmistry/cloud-util/aws"
	_ "github.com/akmistry/cloud-util/gcp"
	_ "github.com/akmistry/cloud-util/local"
	_ "github.com/akmistry/cloud-util/rpc"
)

// Meta-package to import all cloud implementations
