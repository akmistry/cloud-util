package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	//"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
)

type SimpleEmailService struct {
	client *ses.Client
}

func NewDefaultSimpleEmailService() (*SimpleEmailService, error) {
	//credProvider := credentials.NewStaticCredentialsProvider(
	//	*AccessId, *AccessSecret, "")
	cfg, err := config.LoadDefaultConfig(context.TODO())
	//	config.WithRegion(*Region))
	//config.WithCredentialsProvider(credProvider))
	if err != nil {
		return nil, err
	}
	return NewSimpleEmailService(cfg)
}

func NewSimpleEmailService(cfg aws.Config) (*SimpleEmailService, error) {
	c := ses.NewFromConfig(cfg)
	s := &SimpleEmailService{
		client: c,
	}
	return s, nil
}

func (s *SimpleEmailService) Send(address, senderAddress, subject, message string) error {
	charset := "UTF-8"

	in := &ses.SendEmailInput{
		Destination: &types.Destination{
			ToAddresses: []string{address},
		},
		Source: &senderAddress,
		Message: &types.Message{
			Subject: &types.Content{
				Data: &subject,
				// TODO: Fix this
				//Charset: &charset,
			},
			Body: &types.Body{
				Html: &types.Content{
					Data:    &message,
					Charset: &charset,
				},
			},
		},
	}
	_, err := s.client.SendEmail(context.TODO(), in)
	return err
}
