package doctor

import (
	"context"
	"fmt"
	"sort"

	"github.com/wrgl/wrgl/pkg/conf"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

type Doctor struct {
	db   objects.Store
	rs   ref.Store
	tree *Tree
	user conf.User
}

func NewDoctor(db objects.Store, rs ref.Store, user conf.User) *Doctor {
	d := &Doctor{
		db:   db,
		rs:   rs,
		tree: NewTree(db),
		user: user,
	}
	return d
}

type RefIssues struct {
	Ref    string
	Issues []*Issue
}

func (d *Doctor) Diagnose(ctx context.Context, refPrefixes, refNonPrefixes []string) (issues chan *RefIssues, errCh chan error, err error) {
	refs, err := ref.ListLocalRefs(d.rs, refPrefixes, refNonPrefixes)
	if err != nil {
		return
	}
	issues = make(chan *RefIssues, len(refs))
	errCh = make(chan error, 1)
	go func() {
		defer close(issues)
		defer close(errCh)
		for name, sum := range refs {
			select {
			case <-ctx.Done():
				return
			default:
				sl, err := d.diagnoseTree(name, sum)
				if err != nil {
					errCh <- err
					return
				}
				issues <- &RefIssues{
					Ref:    name,
					Issues: sl,
				}
			}
		}
	}()
	return
}

func (d *Doctor) Resolve(issues []*Issue) (err error) {
	sort.Slice(issues, func(i, j int) bool {
		a, b := issues[i], issues[j]
		return a.AncestorCount < b.AncestorCount
	})
	resolver, err := newResolver(d.db, d.tree)
	if err != nil {
		return err
	}
	sum, err := ref.GetRef(d.rs, issues[0].Ref)
	if err != nil {
		return err
	}
	if err = resolver.reset(issues[0], sum); err != nil {
		return err
	}
	for _, iss := range issues {
		if err := resolver.resolveIssue(iss); err != nil {
			return err
		}
	}
	newSum, err := resolver.updateRestOfTree()
	if err != nil {
		return err
	}
	if newSum == nil {
		return fmt.Errorf("new sum is nil")
	}
	return ref.SaveRef(d.rs, issues[0].Ref, newSum, d.user.Name, d.user.Email, "doctor", "resolve", nil)
}
