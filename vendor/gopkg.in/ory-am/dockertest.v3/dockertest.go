package dockertest

import (
	"fmt"
	"github.com/cenk/backoff"
	dc "github.com/fsouza/go-dockerclient"
	"github.com/pkg/errors"
	"time"
	"runtime"
	"os"
)

// Pool represents a connection to the docker API and is used to create and remove docker images.
type Pool struct {
	Client  *dc.Client
	MaxWait time.Duration
}

// Resource represents a docker container.
type Resource struct {
	Container *dc.Container
}

// GetPort returns a resource's published port. You can use it to connect to the service via localhost, e.g. tcp://localhost:1231/
func (r *Resource) GetPort(id string) string {
	if r.Container == nil {
		return ""
	} else if r.Container.NetworkSettings == nil {
		return ""
	}

	m, ok := r.Container.NetworkSettings.Ports[dc.Port(id)]
	if !ok {
		return ""
	} else if len(m) == 0 {
		return ""
	}

	return m[0].HostPort
}

// NewPool creates a new pool. You can pass an empty string to use the default, which is taken from the environment
// variable DOCKER_URL or if that is not defined a sensible default for the operating system you are on.
func NewPool(endpoint string) (*Pool, error) {
	if endpoint == "" {
		if os.Getenv("DOCKER_URL") != "" {
			endpoint = os.Getenv("DOCKER_URL")
		} else if runtime.GOOS == "windows" {
			endpoint = "http://localhost:2375"
		} else {
			endpoint = "unix:///var/run/docker.sock"
		}
	}

	client, err := dc.NewClient(endpoint)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	return &Pool{
		Client: client,
	}, nil
}

// Run starts a docker container.
//
//  pool.Run("mysql", "5.3", []string{"FOO=BAR", "BAR=BAZ"})
func (d *Pool) Run(repository, tag string, env []string) (*Resource, error) {
	if tag == "" {
		tag = "latest"
	}

	_, err := d.Client.InspectImage(fmt.Sprintf("%s:%s", repository, tag))
	if err != nil {
		if err := d.Client.PullImage(dc.PullImageOptions{
			Repository: repository,
			Tag:        tag,
		}, dc.AuthConfiguration{}); err != nil {
			return nil, errors.Wrap(err, "")
		}
	}

	c, err := d.Client.CreateContainer(dc.CreateContainerOptions{
		Config: &dc.Config{
			Image: fmt.Sprintf("%s:%s", repository, tag),
			Env:   env,
		},
		HostConfig: &dc.HostConfig{
			PublishAllPorts: true,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	if err := d.Client.StartContainer(c.ID, nil); err != nil {
		return nil, errors.Wrap(err, "")
	}

	c, err = d.Client.InspectContainer(c.ID)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	return &Resource{
		Container: c,
	}, nil
}


// Purge removes a container and linked volumes from docker.
func (d *Pool) Purge(r *Resource) error {
	if err := d.Client.KillContainer(dc.KillContainerOptions{ID: r.Container.ID}); err != nil {
		return errors.Wrap(err, "")
	}

	if err := d.Client.RemoveContainer(dc.RemoveContainerOptions{ID: r.Container.ID, Force: true, RemoveVolumes: true}); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

// Retry is an exponential backoff retry helper. You can use it to wait for e.g. mysql to boot up.
func (d *Pool) Retry(op func() error) error {
	if d.MaxWait == 0 {
		d.MaxWait = time.Minute
	}
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Second * 5
	bo.MaxElapsedTime = d.MaxWait
	return backoff.Retry(op, bo)
}
