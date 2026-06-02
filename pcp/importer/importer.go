package importer

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	pgimporter "github.com/PlakarKorp/integrations/postgresql/importer"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
)

func init() {
	importer.Register("pcp", location.FLAG_STREAM, NewImporter)
}

type Importer struct {
	pgImporter importer.Importer
}

func NewImporter(appCtx context.Context, opts *connectors.Options, name string, config map[string]string) (importer.Importer, error) {
	config["host"] = "localhost"
	config["port"] = "8888"
	config["username"] = "postgres"
	config["password"] = "postgres"

	pgImporter, err := pgimporter.NewImporter(appCtx, opts, name, config)

	if err != nil {
		return nil, err
	}

	return &Importer{
		pgImporter: pgImporter,
	}, nil
}

func (p *Importer) Import(ctx context.Context, records chan<- *connectors.Record, results <-chan *connectors.Result) error {
	defer close(records)

	// results is passed directly to the sub-importer, which is safe because it
	// never reads from it (not stream-based, no ack needed). If we were chaining
	// multiple sub-importers and one of them consumed acks, passing results directly
	// would be incorrect: acks would need to be relayed one-per-record to whichever
	// sub-importer is currently active.

	// Temporary channel to receive records from the PostgreSQL importer
	pgRecords := make(chan *connectors.Record)
	err := make(chan error, 1)

	go func() {
		err <- p.pgImporter.Import(ctx, pgRecords, results)
	}()

	// Forward records from the PostgreSQL importer to the main records channel
	for rec := range pgRecords {
		rec.Pathname = path.Join("/database", rec.Pathname)
		records <- rec
	}

	if pgErr := <-err; pgErr != nil {
		return pgErr
	}

	var backupPaths = []string{
		"/etc/passwd",
		"/bin",
		"/var/lib/plakman/instance.key",
		"/var/lib/plakman/license.jwt",
	}

	for _, root := range backupPaths {
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				records <- connectors.NewError(p, err)
				return nil
			}
			info, err := d.Info()
			if err != nil {
				records <- connectors.NewError(p, err)
				return nil
			}
			fi := objects.FileInfoFromStat(info)
			var reader func() (io.ReadCloser, error)
			if !d.IsDir() {
				captured := p
				reader = func() (io.ReadCloser, error) {
					return os.Open(captured)
				}
			}
			records <- connectors.NewRecord(path.Join("/data", p), "", fi, nil, reader)
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Importer) Ping(ctx context.Context) error {
	return p.pgImporter.Ping(ctx)
}

func (p *Importer) Close(ctx context.Context) error {
	return p.pgImporter.Close(ctx)
}

func (p *Importer) Root() string   { return "/" }
func (p *Importer) Origin() string { return "pcp" }
func (p *Importer) Type() string   { return "pcp" }

func (p *Importer) Flags() location.Flags {
	return p.pgImporter.Flags()
}
